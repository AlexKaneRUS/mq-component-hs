{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module System.MQ.Component.Extras.Template.Worker
  ( workerController
  , workerScheduler
  ) where

import           Control.Exception                         (SomeException,
                                                            catch)
import           Control.Monad                             (when)
import           Control.Monad.Except                      (liftIO)
import           Data.List                                 (elemIndex)
import           Data.String                               (fromString)
import           System.MQ.Component.Extras.Template.Types (MQAction)
import           System.MQ.Component.Internal.Atomic       (updateLastMsgId)
import           System.MQ.Component.Internal.Config       (load2Channels,
                                                            load3Channels)
import           System.MQ.Component.Internal.Env          (Env (..))
import qualified System.MQ.Component.Internal.Env          as C2 (TwoChannels (..))
import qualified System.MQ.Component.Internal.Env          as C3 (ThreeChannels (..))
import           System.MQ.Error                           (MQError (..),
                                                            errorComponent)
import           System.MQ.Monad                           (MQMonad,
                                                            foreverSafe,
                                                            runMQMonad)
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
workerScheduler :: (MessageLike a, MessageLike b) => MQAction a b -> Env -> MQMonad ()
workerScheduler = worker Scheduler

-- | Given 'WorkerAction' acts as component's communication layer that receives messages of type 'a'
-- from controller, processes them using 'WorkerAction' and sends result of type 'b' to scheduler.
--
workerController :: (MessageLike a, MessageLike b) => MQAction a b -> Env -> MQMonad ()
workerController = worker Controller

--------------------------------------------------------------------------------
-- INTERNAL
--------------------------------------------------------------------------------

-- | We support two types of 'Worker'-like components: ones that receive messages from
-- scheduler and ones that receive messages from controller.
data WorkerType = Scheduler | Controller deriving (Eq, Show)

-- | Alias for function that given environment receives messages from queue.
--
type MessageReceiver = MQMonad (MessageTag, Message)

-- | Given 'WorkerType' and 'WorkerAction' acts as component's communication layer
-- that receives messages of type 'a' from scheduler or controller (depending on 'WorkerType'),
-- processes them using 'WorkerAction' and sends result of type 'b' to scheduler.
--
worker :: forall a b . (MessageLike a, MessageLike b) => WorkerType -> MQAction a b -> Env -> MQMonad ()
worker wType action env@Env{..} = do
    -- Depending on 'WorkerType', we define function using which worker will receive
    -- messages from queue. Also we define channel through which worker will
    -- send messages to queue
    (msgReceiver, schedulerIn) <- msgRecieverAndSchedulerIn

    foreverSafe name $ do
        (tag, Message{..}) <- msgReceiver
        when (checkTag tag) $ updateLastMsgId msgId atomic >> unpackM msgData >>= processTask schedulerIn msgId
  where
    msgRecieverAndSchedulerIn :: MQMonad (MessageReceiver, PushChannel)
    msgRecieverAndSchedulerIn =
      case wType of
        Scheduler  -> load2Channels >>= return . (\x -> (sub $ C2.fromScheduler x, C2.toScheduler x))
        Controller -> load3Channels name >>= return . (\x -> (pull $ C3.fromController x, C3.toScheduler x))

    messageProps :: Props a
    messageProps = props

    checkTag :: MessageTag -> Bool
    checkTag = (`matches` (messageSpec :== fromString (spec messageProps) :&& messageType :== mtype messageProps))

    processTask :: PushChannel -> Hash -> a -> MQMonad ()
    processTask schedulerIn curId config = do
        -- Runtime errors may occur during execution of 'WorkerAction'. In order to process them
        -- without failures we use function 'handleError' that turns 'IOException's into 'MQError's
        responseE <- liftIO $ handleError $ action env config

        case responseE of
          Right response -> createMessage curId creator notExpires response >>= push schedulerIn
          Left  m        -> createMessage curId creator notExpires (MQError errorComponent m) >>= push schedulerIn

    handleError :: MQMonad b -> IO (Either String b)
    handleError valM = (Right <$> runMQMonad valM) `catch` (return . Left . toMeaningfulError)

    toMeaningfulError :: SomeException -> String
    toMeaningfulError e = res
      where
        errorMsg = show e
        indexM = elemIndex '\n' errorMsg

        res = maybe errorMsg (flip take errorMsg) indexM
