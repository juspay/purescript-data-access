module EventuallyConsistentDataStore where

import Prelude

import Data.Maybe (Maybe(..), maybe)
import Data.Time.Duration (Milliseconds)
import DataAccess (class DataStore, fetch, insert, updateDefault)
import Effect.Aff (delay)
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (liftEffect)
import Effect.Ref (Ref)
import Effect.Ref (read, write) as Ref
import Type.Data.Symbol (SProxy(..))

newtype PeriodicallySyncing s m (k :: Symbol) v =
  PeriodicallySyncing
    { getRemoteVersion :: s -> SProxy k -> m Int
    , resolveConflict :: Maybe v -> Maybe v -> Maybe v
    , localVersion :: Ref Int
    , syncInterval :: Milliseconds
    , value :: Ref (Maybe v)
    , validity :: Boolean
    , source :: s }

syncValue :: ∀ s m k v. MonadAff m => DataStore s k m v => PeriodicallySyncing s m k v -> m Unit
syncValue (PeriodicallySyncing r) =
  liftAff
    (delay r.syncInterval *>
    liftEffect (Ref.read r.value))
  >>= maybe
    (pure unit)
    (insert r.source (SProxy :: SProxy k))

instance periodicallySyncingDataStore ::
  (MonadAff m, DataStore s k m v) => DataStore (PeriodicallySyncing s m k v) k m v where
  fetch (PeriodicallySyncing r) prxy =
    if r.validity then liftEffect $ Ref.read r.value else fetch r.source prxy
  insert pr@(PeriodicallySyncing r) prxy v =
    if r.validity
    then liftEffect (Ref.write (pure v) r.value) *> syncValue pr
    else do
      newVersion <- r.resolveConflict <$> fetch r.source prxy <*> liftEffect (Ref.read r.value)
      liftEffect $ Ref.write newVersion r.value
      syncValue pr
  delete pr@(PeriodicallySyncing r) prxy =
    liftEffect (Ref.write Nothing r.value) *> syncValue pr
  update = updateDefault
