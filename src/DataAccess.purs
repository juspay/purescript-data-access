module DataAccess where

import Prelude

import Control.Bind (bindFlipped)
import Control.Monad.State.Class (class MonadState)
import Data.Array (head)
import Data.ByteString (ByteString)
import Data.Map (Map)
import Data.Map (delete, insert, lookup) as Map
import Data.Maybe (Maybe(..), isJust, maybe)
import Data.NonEmpty ((:|))
import Database.Cassandra.Client (Client, execute) as Cassandra
import Database.Cassandra.Types.ResultSet (ResultSet(..))
import Database.Redis (Connection, del, get, hget, hset, set) as Redis
import Effect.Aff.Class (class MonadAff, liftAff)
import Foreign.Object (Object)
import Type.Data.Symbol (class IsSymbol, SProxy, reflectSymbol)

class Monad m <= DataStore s k m v | s -> m, s k -> v where
  fetch :: s -> k -> m (Maybe v)
  insert :: s -> k -> v -> m Unit
  delete :: s -> k -> m Unit
  update :: s -> k -> (Maybe v -> Maybe v) -> m Unit

updateDefault :: ∀ s k m v. DataStore s k m v ⇒ s → k → (Maybe v → Maybe v) → m Unit
updateDefault s k f =
  (f <$> fetch s k) >>=
  maybe
    (delete s k)
    (insert s k)

insertDefault :: ∀ t4 t5 t6 t7. DataStore t4 t5 t6 t7 ⇒ t4 → t5 → t7 → t6 Unit
insertDefault s k v = update s k (const (pure v))

deleteDefault :: ∀ t23 t24 t25 t26. DataStore t23 t24 t25 t26 ⇒ t23 → t24 → t25 Unit
deleteDefault s k = update s k (const Nothing)

exists :: ∀ t10 t13 t6 t7. DataStore t6 t7 t10 t13 ⇒ t6 → t7 → t10 Boolean
exists s = fetch s >>> map isJust

findOne :: ∀ t13 t14 t15 t19. Functor t13 ⇒ DataStore t14 t15 t13 (Array t19) ⇒ t14 → t15 → t13 (Maybe t19)
findOne s k = fetch s k <#> (_ >>= head)

newtype CassandraTable = CassandraTable {client :: Cassandra.Client, table :: String}

class CassandraKey pk where
  toQueryParams :: ∀ b. pk -> {|b}

instance cassandraDataStoreWithPartitionKey :: (CassandraKey pk, MonadAff m) => DataStore CassandraTable pk m (Array a) where
  fetch (CassandraTable {client, table}) key = liftAff $ (pure <<< unwrapResultSet) <$> Cassandra.execute client query (toQueryParams key) mempty
    where
    query = "select * from " <> table <> " where ? = ?"
    unwrapResultSet (ResultSet {rows}) = rows
  insert client prxy = updateDefault client prxy <<< const <<< pure
  delete client prxy = updateDefault client prxy (const Nothing)
  update = updateDefault -- Definitely will lead to an infinite loop at runtime, define either update or insert and delete or both

class FromByteString v where
  decodeByteString :: ByteString -> Maybe v

class ToByteString v where
  encodeByteString :: v -> ByteString

class (FromByteString v, ToByteString v) <= ByteStringRep v

data RecordField a b = RecordField { record :: a, field :: b }

instance redisNestedKVDataStore :: (MonadAff m, ToByteString rec, ToByteString field, ByteStringRep v) => DataStore Redis.Connection (RecordField rec field) m v where
  fetch client (RecordField { record, field }) = liftAff $ (bindFlipped decodeByteString) <$> Redis.hget client (encodeByteString record) (encodeByteString field)
  insert client (RecordField { record, field }) v = liftAff $ void $ Redis.hset client (encodeByteString record) (encodeByteString field) (encodeByteString v)
  delete client k = pure unit -- Redis client doesn't have a hdel implementation. TODO
  update = updateDefault

else instance redisKVDataStore :: (MonadAff m, ToByteString k, ByteStringRep v) => DataStore Redis.Connection k m v where
  fetch client =  map (bindFlipped decodeByteString) <<< liftAff <<< Redis.get client <<< encodeByteString
  insert client k v = liftAff $ Redis.set client (encodeByteString k) (encodeByteString v) Nothing Nothing
  delete client k = liftAff $ Redis.del client ((encodeByteString k) :| mempty)
  update = updateDefault

instance mapIsDataStore :: (Ord k, Monad m) => DataStore (Map k v) k m v where
  fetch map k = pure $ Map.lookup k map
  insert map k v = void $ pure $ Map.insert k v map
  delete map k = void $ pure $ Map.delete k map
  update = updateDefault
