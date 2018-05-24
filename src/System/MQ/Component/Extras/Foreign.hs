{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module System.MQ.Component.Extras.Foreign
 ( callForeignComponent
 ) where

import           Control.Monad.Except                (catchError, liftIO)
import           Control.Monad.Fix                   (fix)
import           Data.ByteString                     (ByteString)
import qualified Data.ByteString                     as BS (null)
import qualified Data.ByteString.Char8               as BSC8 (unpack)
import           System.Log.Logger                   (infoM)
import           System.MQ.Component.Extras.Error    (throwForeignError)
import           System.MQ.Component.Internal.Config (load2ChannelsWithContext)
import           System.MQ.Component.Internal.Env    (Env (..),
                                                      TwoChannels (..))
import           System.MQ.Error                     (MQError (..))
import           System.MQ.Monad                     (MQMonad)
import           System.MQ.Protocol                  (Hash, Message (..),
                                                      MessageLike (..),
                                                      MessageTag, Timestamp,
                                                      createMessage, messagePid)
import           System.MQ.Transport                 (SubChannel, closeM,
                                                      contextM, push, sub,
                                                      terminateM)

-- | Allows user to send message to queue and receive response to it.
-- IMPORTANT: in MoniQue should exist and be running component that will
-- respond to that message, otherwise call will stuck in an infinite loop.
callForeignComponent :: forall a b . (MessageLike a, MessageLike b) => Env       -- ^ 'Env' of component
                                                                    -> Hash      -- ^ id of message that begets message that will be send
                                                                    -> Timestamp -- ^ expiration date of message that will be sent to foreign component
                                                                    -> a         -- ^ data that will be sent in message
                                                                    -> MQMonad b -- ^ result of foreign component's computation
callForeignComponent Env{..} curId expires mdata = do
    context' <- contextM
    TwoChannels{..} <- load2ChannelsWithContext context'

    dataMsg@Message{..} <- createMessage curId creator expires mdata

    liftIO $ infoM name $ "FOREIGN CALL: Sending message with id " ++ BSC8.unpack msgId ++ " to queue"
    push toScheduler dataMsg

    responseData <- receiveResponse fromScheduler msgId
    liftIO $ infoM name $ "FOREIGN CALL: Received response for message with id " ++ BSC8.unpack msgId ++ " from queue"

    -- Close sockets and context that they are opened in
    closeM fromScheduler >> closeM toScheduler >> terminateM context'

    return responseData

  where
    -- Receives messages from queue until message with given pId is received
    receiveResponse :: SubChannel -> Hash -> MQMonad b
    receiveResponse schedulerOut pId = fix $ \action -> do
        (tag, Message{..}) <- sub schedulerOut `catchError` handleSub

        -- If message's tag is empty, that means message is broken and
        -- we should wait for next message. Otherwise we check whether
        -- tag's pId matches given 'pId'
        if not (BS.null tag) && checkTag pId tag
          then unpackM msgData `catchError` const (errorMsgToError msgData)
          else action

    checkTag :: Hash -> MessageTag -> Bool
    checkTag pId = (== pId) . messagePid

    -- | If message received from queue can't be decoded into MoniQue message,
    -- then we set its tag to empty.
    handleSub :: MQError -> MQMonad (MessageTag, Message)
    handleSub _ = return ("", error "Received broken message.")

    errorMsgToError :: ByteString -> MQMonad b
    errorMsgToError bs = unpackM bs >>= throwForeignError . errorMessage
