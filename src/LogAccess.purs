module LogAccess where

import Data.Either.Nested (type (\/))
import Prelude

import Control.Monad.Error.Class (class MonadError, withResource)
import Control.Monad.Except.Trans (runExceptT)
import Control.Monad.Trans.Class (lift)
import Data.Traversable (class Traversable, for)

class Monad m <= Log s k v m | s -> m, s k -> v where
  tell :: s -> k -> v -> m s

type CheckpointedAction m v
  = { perform :: m v
    , checkpointEntry :: String
    -- TODO : Add a optional Rollback
    }

class Monad m <= CheckpointProvider s k m | s -> m where
  addCheckpoint :: s -> k -> String -> m Unit
  removeCheckpoint :: s -> k -> String -> m Unit

runTransactionWithCheckpoint
  :: ∀ e s k t m v
  . Traversable t
  ⇒ MonadError e m
  ⇒ CheckpointProvider s k m
  ⇒ s
  → k
  → t (CheckpointedAction m v) → m (e \/ t v)
runTransactionWithCheckpoint s k actions =
  runExceptT $
    for actions (\{ perform, checkpointEntry } ->
      lift $ withResource
        (addCheckpoint s k checkpointEntry)
        (const $ removeCheckpoint s k checkpointEntry)
        (const perform))
