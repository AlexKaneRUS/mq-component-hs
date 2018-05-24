{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module System.MQ.Component.Extras.Template.Worker
  ( workerController, workerControllerS
  , workerScheduler, workerSchedulerS
  ) where

import           Control.Exception                         (SomeException,
                                                            catch)
import           Control.Monad                             (when)
import           Control.Monad.Except                      (liftIO)
import           Control.Monad.State.Strict                (get)
import           Data.List                                 (elemIndex)
import           System.MQ.Component.Extras.Template.Types (MQAction, MQActionS)
import           System.MQ.Component.Internal.Atomic       (updateLastMsgId)
import           System.MQ.Component.Internal.Config       (load2Channels,
                                                            load3Channels)
import           System.MQ.Component.Internal.Env          (Env (..))
import qualified System.MQ.Component.Internal.Env          as C2 (TwoChannels (..))
import qualified System.MQ.Component.Internal.Env          as C3 (ThreeChannels (..))
import           System.MQ.Error                           (MQError (..),
                                                            errorComponent)
import           System.MQ.Monad                           (MQMonad, MQMonadS,
                                                            foreverSafe,
                                                            runMQMonadS)
import           System.MQ.Protocol                        (Condition (..),
                                                            Hash, Message (..),
                                                            MessageLike (..),
                                                            MessageTag,
                                                            Props (..),
                                                            createMessage,
                                                            matches,
                                                            messageSpec,
                                                            messageType,
                                                            notExpires)
import           System.MQ.Transport                       (PushChannel, pull,
                                                            push, sub)

-- | Given 'WorkerAction' acts as component's communication layer that receives messages of type 'a'
-- from scheduler, processes them using 'WorkerAction' and sends result of type 'b' back to scheduler.
--
workerSchedulerS :: (MessageLike a, MessageLike b) => MQActionS s a b -> Env -> MQMonadS s ()
workerSchedulerS = worker Scheduler

workerScheduler :: (MessageLike a, MessageLike b) => MQAction a b -> Env -> MQMonad ()
workerScheduler action env = workerSchedulerS action env

-- | Given 'WorkerAction' acts as component's communication layer that receives messages of type 'a'
-- from controller, processes them using 'WorkerAction' and sends result of type 'b' to scheduler.
--
workerControllerS :: (MessageLike a, MessageLike b) => MQActionS s a b -> Env -> MQMonadS s ()
workerControllerS = worker Controller

workerController :: (MessageLike a, MessageLike b) => MQAction a b -> Env -> MQMonad ()
workerController action env = workerControllerS action env

--------------------------------------------------------------------------------
-- INTERNAL
--------------------------------------------------------------------------------

-- | We support two types of 'Worker'-like components: ones that receive messages from
-- scheduler and ones that receive messages from controller.
data WorkerType = Scheduler | Controller deriving (Eq, Show)

-- | Alias for function that given environment receives messages from queue.
--
type MessageReceiver s = MQMonadS s (MessageTag, Message)

-- | Given 'WorkerType' and 'WorkerAction' acts as component's communication layer
-- that receives messages of type 'a' from scheduler or controller (depending on 'WorkerType'),
-- processes them using 'WorkerAction' and sends result of type 'b' to scheduler.
--
worker :: forall a b s . (MessageLike a, MessageLike b) => WorkerType -> MQActionS s a b -> Env -> MQMonadS s ()
worker wType action env@Env{..} = do
    -- Depending on 'WorkerType', we define function using which worker will receive
    -- messages from queue. Also we define channel through which worker will
    -- send messages to queue
    (msgReceiver, schedulerIn) <- msgRecieverAndSchedulerIn

    foreverSafe name $ do
        (tag, Message{..}) <- msgReceiver
        state <- get
        when (checkTag tag) $ updateLastMsgId msgId atomic >> unpackM msgData >>= processTask state schedulerIn msgId
  where
    msgRecieverAndSchedulerIn :: MQMonadS s (MessageReceiver s, PushChannel)
    msgRecieverAndSchedulerIn =
      case wType of
        Scheduler  -> load2Channels >>= return . (\x -> (sub $ C2.fromScheduler x, C2.toScheduler x))
        Controller -> load3Channels name >>= return . (\x -> (pull $ C3.fromController x, C3.toScheduler x))

    messageProps :: Props a
    messageProps = props

    checkTag :: MessageTag -> Bool
    checkTag = (`matches` (messageSpec :== spec messageProps :&& messageType :== mtype messageProps))

    processTask :: s -> PushChannel -> Hash -> a -> MQMonadS s ()
    processTask state schedulerIn curId config = do
        -- Runtime errors may occur during execution of 'WorkerAction'. In order to process them
        -- without failures we use function 'handleError' that turns 'IOException's into 'MQError's
        responseE <- liftIO $ handleError state $ action env config

        case responseE of
          Right response -> createMessage curId creator notExpires response >>= push schedulerIn
          Left  m        -> createMessage curId creator notExpires (MQError errorComponent m) >>= push schedulerIn

    handleError :: s -> MQMonadS s b -> IO (Either String b)
    handleError state valM = (Right . fst <$> runMQMonadS valM state) `catch` (return . Left . toMeaningfulError)

    toMeaningfulError :: SomeException -> String
    toMeaningfulError e = res
      where
        errorMsg = show e
        indexM = elemIndex '\n' errorMsg

        res = maybe errorMsg (flip take errorMsg) indexM
