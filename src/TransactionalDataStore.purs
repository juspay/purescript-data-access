module TransactionalDataStore where

import Prelude

import DataAccess (class DataStore)
import Database.Redis (Connection)
import Effect.Aff.Class (class MonadAff)

class MonadAff m <= DistributedLockProvider m s k | s -> m where
  tryLock :: s -> k -> m Boolean
  unlock :: s -> k -> m Unit
  acquire :: s -> k -> m Unit

atomicallyPerformWith :: ∀ a s1 s2 k1 k2 m v. DataStore s1 k1 m v => DistributedLockProvider m s2 k2 => s2 -> k2 -> m a -> m a
atomicallyPerformWith lp lock action = acquire lp lock *> action <* unlock lp lock

{--instance redisDistributedLockProvider :: MonadAff m => DistributedLockProvider m Redis.Connection String where--}
