module DataAccess where

import Prelude

import Data.ByteString (fromUTF8, toUTF8) as BS
import Data.Maybe (Maybe(..), isJust, maybe)
import Database.Cassandra.Client (Client, execute) as Cassandra
import Database.Cassandra.Types.ResultSet (ResultSet(..))
import Database.Redis (Connection, lrange) as Redis
import Effect.Aff.Class (class MonadAff, liftAff)
import Type.Data.Symbol (class IsSymbol, SProxy, reflectSymbol)

class (IsSymbol k, Monad m) <= DataStore s k m v | s -> m, s k -> v where
  fetch :: s -> SProxy k -> m (Maybe v)
  insert :: s -> SProxy k -> v -> m Unit
  delete :: s -> SProxy k -> m Unit
  update :: s -> SProxy k -> (Maybe v -> Maybe v) -> m Unit

updateDefault :: ∀ s k m v. DataStore s k m v ⇒ s → SProxy k → (Maybe v → Maybe v) → m Unit
updateDefault s k f =
  (f <$> fetch s k) >>=
  maybe
    (delete s k)
    (insert s k)

insertDefault :: ∀ t4 t5 t6 t7. DataStore t4 t5 t6 t7 ⇒ t4 → SProxy t5 → t7 → t6 Unit
insertDefault s k v = update s k (const (pure v))

deleteDefault :: ∀ t23 t24 t25 t26. DataStore t23 t24 t25 t26 ⇒ t23 → SProxy t24 → t25 Unit
deleteDefault s k = update s k (const Nothing)

exists :: ∀ t10 t13 t6 t7. DataStore t6 t7 t10 t13 ⇒ t6 → SProxy t7 → t10 Boolean
exists s = fetch s >>> map isJust

data Transaction = Transaction String

newtype CassandraTable = CassandraTable {client :: Cassandra.Client, table :: String}

instance cassandraClientTransactionsForOrderId :: MonadAff m => DataStore CassandraTable "orderId" m (Array Transaction) where
  fetch (CassandraTable {client, table}) prxy = liftAff $ (pure <<< unwrapResultSet) <$> Cassandra.execute client query {orderId: reflectSymbol prxy} mempty
    where
    query = "select * from " <> table <> " where ? = ?"
    unwrapResultSet (ResultSet {rows}) = rows
  insert client prxy = updateDefault client prxy <<< const <<< pure
  delete client prxy = updateDefault client prxy (const Nothing)
  update = updateDefault -- Definitely will lead to an infinite loop at runtime, define either update or insert and delete or both

instance redisListForTransactionsDataStore :: MonadAff m => DataStore Redis.Connection "orderId" m (Array Transaction) where
  fetch client prxy =
    liftAff $
      (Just <<< map byteStringToTransaction) <$> Redis.lrange client (BS.toUTF8 (reflectSymbol prxy)) 0 (-1)
    where
    byteStringToTransaction = BS.fromUTF8 >>> Transaction
  insert client prxy = updateDefault client prxy <<< const <<< pure
  delete client prxy = updateDefault client prxy (const Nothing)
  update = updateDefault -- Definitely will lead to an infinite loop at runtime, define either update or insert and delete or both
