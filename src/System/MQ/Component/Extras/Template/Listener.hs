{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module System.MQ.Component.Extras.Template.Listener
  ( listenerComm
  , listenerTech
  ) where

import           Control.Monad                             (when)
import           Data.String                               (fromString)
import           System.MQ.Component.Extras.Template.Types (MQActionVoid)
import           System.MQ.Component.Internal.Atomic       (updateLastMsgId)
import           System.MQ.Component.Internal.Config       (load2Channels,
                                                            loadTechChannels)
import           System.MQ.Component.Internal.Env          (Env (..),
                                                            TwoChannels (..))
import           System.MQ.Monad                           (MQMonad,
                                                            foreverSafe)
import           System.MQ.Protocol                        (Condition (..),
                                                            Message (..),
                                                            MessageLike (..),
                                                            MessageTag,
                                                            Props (..),
                                                            emptyHash, matches,
                                                            messageSpec,
                                                            messageType)
import           System.MQ.Transport                       (SubChannel, sub)


-- | Listener of communication level's scheduler
--
listenerComm :: MessageLike a => MQActionVoid a -> Env -> MQMonad ()
listenerComm analyser env = load2Channels >>= listenerWithChannels analyser env

-- | Listener of technical level's scheduler
--
listenerTech :: MessageLike a => MQActionVoid a -> Env -> MQMonad ()
listenerTech analyser env = loadTechChannels >>= listenerWithChannels analyser env

-- | Template `listener` allows user to gather data from queue and perform different actions with it
--
listenerWithChannels :: MessageLike a => MQActionVoid a -> Env -> TwoChannels -> MQMonad ()
listenerWithChannels analyser env@Env{..} TwoChannels{..} = foreverSafe name $ obtainData env fromScheduler analyser

-- | Receive from queue message with given type, spec and pId and process it using 'MQActionVoid'
--
obtainData :: forall a . MessageLike a => Env -> SubChannel -> MQActionVoid a -> MQMonad ()
obtainData env@Env{..} subChannel analyser = do
    (tag, Message{..}) <- sub subChannel

    when (filterTag tag) $ do
      -- Set 'lastMsgId' to id of message that worker will process
      updateLastMsgId msgId atomic

      -- Process data from message using 'action'
      unpackM msgData >>= analyser env

      -- After message has been processed, clear 'lastMsgId'
      updateLastMsgId emptyHash atomic

  where
    Props{..} = props :: Props a

    mType = mtype
    mSpec = fromString $ spec

    filterTag :: MessageTag -> Bool
    filterTag = (`matches` (messageType :== mType :&& messageSpec :== mSpec))

