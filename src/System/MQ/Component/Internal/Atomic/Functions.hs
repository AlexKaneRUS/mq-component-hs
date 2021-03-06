module System.MQ.Component.Internal.Atomic.Functions
  (
    createAtomic
  , createAtomicMVar
  , getLastMsgId
  , updateIsAlive
  , updateMessage
  , updateLastMsgId
  , tryLastMsgId
  , tryIsAlive
  , tryMessage
  ) where

import           Control.Concurrent                        (ThreadId)
import           Control.Concurrent.MVar                   (MVar, modifyMVar_,
                                                            newMVar,
                                                            tryReadMVar)
import           Control.Lens                              (set, view)
import           Control.Monad                             ((>=>))
import           Control.Monad.Except                      (throwError)
import           Control.Monad.IO.Class                    (MonadIO, liftIO)
import           System.MQ.Component.Internal.Atomic.Types (Atomic (..),
                                                            IsAlive, isAlive,
                                                            lastMsgId, message)
import           System.MQ.Monad                           (MQError (..),
                                                            MQMonad)
import           System.MQ.Protocol                        (Hash, emptyHash)

-- | Creates new 'Atomic' with information only about communication 'ThreadId'.
--
createAtomic :: ThreadId -> Atomic
createAtomic tid = Atomic tid True "" emptyHash

-- | Creates 'Atomic' in monad.
--
createAtomicMVar :: MonadIO m => ThreadId -> m (MVar Atomic)
createAtomicMVar = liftIO . newMVar . createAtomic

-- | Updates isAlive field in 'Atomic'.
--
updateIsAlive :: MonadIO m => IsAlive -> MVar Atomic -> m ()
updateIsAlive st atomic = liftIO $ modifyMVar_ atomic (pure . set isAlive st)

-- | Updates message field in 'Atomic'.
--
updateMessage :: MonadIO m => String -> MVar Atomic -> m ()
updateMessage ms atomic = liftIO $ modifyMVar_ atomic (pure . set message ms)

-- | Updates lastMsg field in 'Atomic'.
--
updateLastMsgId  :: MonadIO m => Hash -> MVar Atomic -> m ()
updateLastMsgId lmsg atomic = liftIO $ modifyMVar_ atomic (pure . set lastMsgId lmsg)

-- | Returns lastMsg from 'Atomic'.
--
tryLastMsgId :: MonadIO m => MVar Atomic -> m (Maybe Hash)
tryLastMsgId = (liftIO . tryReadMVar) >=> pure . fmap (view lastMsgId)

-- | Returns lastMsg from 'Atomic'. Throws an error if 'Atomic' is empty.
--
getLastMsgId :: MVar Atomic -> MQMonad Hash
getLastMsgId = fromMMQ . tryLastMsgId
  where
    fromMMQ :: MQMonad (Maybe Hash) -> MQMonad Hash
    fromMMQ = (maybe (throwError $ MQComponentError "Can't get message id because from Atomic.") pure =<<)

-- | Returns isAlive from 'Atomic' if possible.
--
tryIsAlive :: MonadIO m => MVar Atomic -> m (Maybe IsAlive)
tryIsAlive = (liftIO . tryReadMVar) >=> pure . fmap (view isAlive)

-- | Returns message from 'Atomic' if possible.
--
tryMessage :: MonadIO m => MVar Atomic -> m (Maybe String)
tryMessage = (liftIO . tryReadMVar) >=> pure . fmap (view message)

