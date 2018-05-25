module System.MQ.Component.Extras.Error
  ( throwComponentError
  , throwForeignError
  ) where

import           Control.Monad.Except (throwError)
import           System.MQ.Error      (MQError (..), errorComponent,
                                       errorForeign)
import           System.MQ.Monad      (MQMonadS)

throwComponentError :: String -> MQMonadS s a
throwComponentError = throwError . MQError errorComponent

throwForeignError :: String -> MQMonadS s a
throwForeignError = throwError . MQError errorForeign
