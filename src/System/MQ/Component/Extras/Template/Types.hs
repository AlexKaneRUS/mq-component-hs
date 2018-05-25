module System.MQ.Component.Extras.Template.Types
  (
    MQAction
  , MQActionS
  , MQActionVoid
  , MQActionVoidS
  ) where

import           System.MQ.Component.Internal.Env (Env)
import           System.MQ.Monad                  (MQMonadS)

-- | Type of function that user needs to define in order to use some templates.
-- Function takes component's 'Env' and data of type 'a', processes given data
-- and returns result of type 'b'. This version of action supports state.
--
type MQActionS s a b = Env -> a -> MQMonadS s b

-- | Action without state.
--
type MQAction a b = MQActionS () a b

-- | Type of function that user needs to define in order to use some templates.
-- Type is similar to 'MQAction' apart from the fact that this action doesn't
-- produce any results.
--
type MQActionVoidS s a = MQActionS s a ()

type MQActionVoid a = MQAction a ()
