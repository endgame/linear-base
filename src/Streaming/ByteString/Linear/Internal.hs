{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE LinearTypes #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Streaming.ByteString.Linear.Internal where

import qualified Control.Functor.Linear as Control
import Control.Monad.IO.Class.Linear (MonadIO (..))
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import Data.Functor.Identity (Identity)
import qualified Data.Functor.Linear as Data
import Data.Monoid (Ap (..))
import Data.Monoid.Linear (Monoid (..), Semigroup (..))
import Data.String (IsString (..))
import Data.Word (Word8)
import Foreign.Ptr (plusPtr)
import Foreign.Storable (peek, poke, sizeOf)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import GHC.Types (SPEC (..))
import Prelude.Linear
import Prelude.Linear (Int, otherwise, ($), (.))
import Streaming.Linear (Of (..), Stream (..), destroy)
import qualified Streaming.Prelude.Linear as S
import System.IO.Unsafe (unsafePerformIO)
import qualified Prelude

data ByteStream m r where
  Empty :: r %1 -> ByteStream m r
  Chunk :: {-# UNPACK #-} !ByteString -> ByteStream m r %1 -> ByteStream m r
  Go :: m (ByteStream m r) %1 -> ByteStream m r
  deriving (Semigroup, Monoid) via (Ap (ByteStream m) r)

instance (Data.Functor m) => Data.Functor (ByteStream m) where
  fmap :: (a %1 -> b) -> ByteStream m a %1 -> ByteStream m b
  fmap f = \case
    Empty a -> Empty (f a)
    Chunk bs bss -> Chunk bs (Data.fmap f bss)
    Go mbss -> Go (Data.fmap (Data.fmap f) mbss)

instance (Control.Functor m) => Data.Applicative (ByteStream m) where
  pure :: a -> ByteStream m a
  pure = Empty

  (<*>) :: ByteStream m (a %1 -> b) %1 -> ByteStream m a %1 -> ByteStream m b
  bf0 <*> bx0 = loop SPEC bx0 bf0
    where
      loop :: SPEC -> ByteStream m a %1 -> ByteStream m (a %1 -> b) %1 -> ByteStream m b
      loop !_ bx bf = case bf of
        Empty f -> Control.fmap f bx
        Chunk bs bss -> Chunk bs (loop SPEC bx bss)
        Go m -> Go $ Control.fmap (loop SPEC bx) m

instance (Control.Functor m) => Control.Functor (ByteStream m) where
  fmap :: (a %1 -> b) %1 -> ByteStream m a %1 -> ByteStream m b
  fmap f = \case
    Empty a -> Empty (f a)
    Chunk bs bss -> Chunk bs (Control.fmap f bss)
    Go mbss -> Go (Control.fmap (Control.fmap f) mbss)

instance (Control.Functor m) => Control.Applicative (ByteStream m) where
  pure :: a %1 -> ByteStream m a
  pure = Empty
  {-# INLINE pure #-}

  (<*>) :: ByteStream m (a %1 -> b) %1 -> ByteStream m a %1 -> ByteStream m b
  (<*>) = (Data.<*>)
  {-# INLINE (<*>) #-}

instance (Control.Monad m) => Control.Monad (ByteStream m) where
  (>>=) :: ByteStream m a %1 -> (a %1 -> ByteStream m b) %1 -> ByteStream m b
  x >>= f = case x of
    Empty a -> f a
    Chunk bs bss -> Chunk bs (bss Control.>>= f)
    Go m -> Go (Control.fmap (Control.>>= f) m)
  {-# INLINEABLE (>>=) #-}

  (>>) :: ByteStream m () %1 -> ByteStream m a %1 -> ByteStream m a
  x0 >> y0 = loop SPEC y0 x0
    where
      loop :: SPEC -> ByteStream m a %1 -> ByteStream m () %1 -> ByteStream m a
      loop !_ y x = case x of
        Empty () -> y
        Chunk a b -> Chunk a (loop SPEC y b)
        Go m -> Go (Control.fmap (loop SPEC y) m)
  {-# INLINE (>>) #-}

instance (MonadIO m) => MonadIO (ByteStream m) where
  liftIO = Go . Control.fmap Empty . liftIO
  {-# INLINE liftIO #-}

instance Control.MonadTrans ByteStream where
  lift :: (Control.Functor m) => m r %1 -> ByteStream m r
  lift = Go . Control.fmap Control.pure
  {-# INLINE lift #-}

instance (r ~ ()) => IsString (ByteStream m r) where
  fromString s = chunk (B.pack (Prelude.map B.c2w s))
  {-# INLINE fromString #-}

deriving instance
  (m ~ Identity, Prelude.Show r) => Prelude.Show (ByteStream m r)

-- | A la @hoist@ from package
-- [mmorph](https://hackage.haskell.org/package/mmorph), but no linear
-- @MFunctor@ exists yet.
hoist ::
  forall m n r.
  (Control.Functor m) =>
  (forall a. m a %1 -> n a) ->
  ByteStream m r %1 ->
  ByteStream n r
hoist f = loop
  where
    loop :: ByteStream m r %1 -> ByteStream n r
    loop = \case
      Empty r -> Empty r
      Chunk bs bss -> Chunk bs $ loop bss
      Go m -> Go . f $ Control.fmap loop m
{-# INLINEABLE hoist #-}

consChunk :: ByteString -> ByteStream m r %1 -> ByteStream m r
consChunk b bs
  | B.null b = bs
  | otherwise = Chunk b bs
{-# INLINE consChunk #-}

chunk :: ByteString -> ByteStream m ()
chunk bs = consChunk bs (Empty ())
{-# INLINE chunk #-}

mwrap :: m (ByteStream m r) %1 -> ByteStream m r
mwrap = Go
{-# INLINE mwrap #-}

materialize ::
  (forall x. (r %1 -> x) -> (ByteString -> x %1 -> x) -> (m x %1 -> x) -> x) ->
  ByteStream m r
materialize f = f Empty Chunk Go
{-# INLINE [0] materialize #-}

dematerialize ::
  forall m r.
  (Data.Functor m) =>
  ByteStream m r %1 ->
  (forall x. (r %1 -> x) -> (ByteString -> x %1 -> x) -> (m x %1 -> x) -> x)
dematerialize x0 nil0 cons0 mwrap0 = loop SPEC nil0 cons0 mwrap0 x0
  where
    loop :: forall x. SPEC -> (r %1 -> x) -> (ByteString -> x %1 -> x) -> (m x %1 -> x) -> ByteStream m r %1 -> x
    loop !_ nil cons mwrap = go
      where
        go :: ByteStream m r %1 -> x
        go = \case
          Empty r -> nil r
          Chunk bs bss -> cons bs (go bss)
          Go m -> mwrap $ Data.fmap go m
{-# INLINE [1] dematerialize #-}

{-# RULES
"dematerialize/materialize" forall (phi :: forall b. (r %1 -> b) -> (B.ByteString -> b %1 -> b) -> (m b %1 -> b) -> b). dematerialize (materialize phi) = phi
  #-}

-- | The chunk size used for I\/O. Currently set to 32k, less the memory management overhead
defaultChunkSize :: Int
defaultChunkSize = 32 * k - chunkOverhead
  where
    k = 1024
{-# INLINE defaultChunkSize #-}

-- | The recommended chunk size. Currently set to 4k, less the memory management overhead
smallChunkSize :: Int
smallChunkSize = 4 * k - chunkOverhead
  where
    k = 1024
{-# INLINE smallChunkSize #-}

-- | The memory management overhead. Currently this is tuned for GHC only.
chunkOverhead :: Int
chunkOverhead = 2 * sizeOf (undefined :: Int)
{-# INLINE chunkOverhead #-}

-- | Consume the chunks of an effectful `ByteString` with a natural right fold.
foldrChunks ::
  (Control.Monad m, Consumable r) =>
  (B.ByteString -> a %1 -> a) ->
  a ->
  ByteStream m r %1 ->
  m a
foldrChunks step nil bs =
  dematerialize
    bs
    ( \r -> case consume r of
        () -> Control.pure nil
    )
    (\b -> Control.fmap (step b))
    Control.join
{-# INLINE foldrChunks #-}

-- | Consume the chunks of an effectful 'ByteString' with a left fold. Suitable
-- for use with 'S.mapped'.
foldlChunks ::
  (Control.Monad m) =>
  (a -> B.ByteString -> a) ->
  a ->
  ByteStream m r %1 ->
  m (Of a r)
foldlChunks f z = go z
  where
    go !a = \case
      Empty r -> Control.pure (a :> r)
      Chunk c cs -> go (f a c) cs
      Go m -> m Control.>>= go a
{-# INLINEABLE foldlChunks #-}

-- | Instead of mapping over each `Word8` or `Char`, map over each strict
-- `B.ByteString` chunk in the stream.
chunkMap ::
  (Control.Monad m) =>
  (ByteString -> ByteString) ->
  ByteStream m r %1 ->
  ByteStream m r
chunkMap f bs = dematerialize bs Control.pure (\b -> Chunk (f b)) Go
{-# INLINE chunkMap #-}

-- | Like `chunkMap`, but map effectfully.
chunkMapM ::
  (Control.Monad m) =>
  (ByteString -> m (Ur ByteString)) ->
  ByteStream m r %1 ->
  ByteStream m r
chunkMapM f bs =
  dematerialize
    bs
    Control.pure
    ( \chunk rest -> Go $ Control.do
        Ur chunk' <- f chunk
        Control.pure (Chunk chunk' rest)
    )
    Go
{-# INLINE chunkMapM #-}

-- | @chunkFold@ is preferable to @foldlChunks@ since it is an appropriate
-- argument for @Control.Foldl.purely@ which permits many folds and sinks to be
-- run simultaneously on one bytestream.
chunkFold ::
  (Control.Monad m) =>
  (x -> B.ByteString -> x) ->
  x ->
  (x -> a) ->
  ByteStream m r %1 ->
  m (Of a r)
chunkFold step begin done = go begin
  where
    go !a = \case
      Empty r -> Control.pure (done a :> r)
      Chunk c cs -> go (step a c) cs
      Go m -> m Control.>>= go a
{-# INLINEABLE chunkFold #-}

-- | 'chunkFoldM' is preferable to 'foldlChunksM' since it is an appropriate
-- argument for 'Control.Foldl.impurely' which permits many folds and sinks to
-- be run simultaneously on one bytestream.
--
-- TODO: Does this need a linear version of Control.Foldl?
--
-- chunkFoldM ::
--   (Control.Monad m) =>
--   (x -> B.ByteString -> m (Ur x)) ->
--   m (Ur x) ->
--   (x -> m (Ur a)) ->
--   ByteStream m r %1 ->
--   m (Of a r)
-- chunkFoldM step begin done bs = Control.do
--   Ur x <- begin
--   go x bs
--   where
--     go !x = \case
--       Empty r -> Control.do
--         Ur a <- done x
--         Control.pure (a :> r)
--       Chunk c cs -> Control.do
--         Ur x' <- step x c
--         go x' cs
--       Go m -> m Control.>>= go x
-- {-# INLINEABLE chunkFoldM #-}

-- | Consume the chunks of an effectful ByteString with a natural right monadic fold.
foldrChunksM ::
  (Control.Monad m, Consumable r) =>
  (ByteString -> m a %1 -> m a) ->
  m a ->
  ByteStream m r %1 ->
  m a
foldrChunksM step nil bs =
  dematerialize bs (\r -> case consume r of () -> nil) step Control.join
{-# INLINE foldrChunksM #-}

-- | Internal utility for @unfoldr@.
unfoldrNE :: Int -> (a -> Either r (Word8, a)) -> a -> (B.ByteString, Either r a)
unfoldrNE i f x0
  | i < 0 = (B.empty, Right x0)
  | otherwise = unsafePerformIO $ B.createAndTrim' i $ \p -> go p x0 0
  where
    go !p !x !n
      | n == i = Prelude.pure (0, n, Right x)
      | otherwise = case f x of
          Left r -> Prelude.pure (0, n, Left r)
          Right (w, x') -> do
            poke p w
            go (p `plusPtr` 1) x' (n + 1)
{-# INLINE unfoldrNE #-}

-- | Given some continual monadic action that produces strict `ByteString`
-- chunks, produce a stream of bytes.
unfoldMChunks ::
  (Control.Monad m) =>
  (s %1 -> m (Maybe (Ur B.ByteString, s))) ->
  s %1 ->
  ByteStream m ()
unfoldMChunks step = loop
  where
    loop s = Go $ Control.do
      m <- step s
      Control.pure $ case m of
        Nothing -> Empty ()
        Just (Ur bs, s') -> Chunk bs (loop s')
{-# INLINEABLE unfoldMChunks #-}

-- | Like `unfoldMChunks`, but feed through a final @r@ return value.
unfoldrChunks ::
  (Control.Monad m) =>
  (s %1 -> m (Either r (Ur B.ByteString, s))) ->
  s %1 ->
  ByteStream m r
unfoldrChunks step = loop
  where
    loop !s = Go $ Control.do
      m <- step s
      Control.pure $ case m of
        Left r -> Empty r
        Right (Ur bs, s') -> Chunk bs (loop s')
{-# INLINEABLE unfoldrChunks #-}

-- | Stream chunks from something that contains @m (Maybe ByteString)@ until it
-- returns 'Nothing'. 'reread' is of particular use rendering @io-streams@ input
-- streams as byte streams in the present sense.
--
-- > import qualified Data.ByteString as B
-- > import qualified System.IO.Streams as S
-- > Q.reread S.read            :: S.InputStream B.ByteString -> Q.ByteStream IO ()
-- > Q.reread (liftIO . S.read) :: MonadIO m => S.InputStream B.ByteString -> Q.ByteStream m ()
--
-- The other direction here is
--
-- > S.unfoldM Q.unconsChunk    :: Q.ByteString IO r -> IO (S.InputStream B.ByteString)
reread ::
  (Control.Monad m) =>
  (s -> m (Maybe (Ur ByteString))) ->
  s ->
  ByteStream m ()
reread step s = loop
  where
    loop = Go $ Control.do
      m <- step s
      Control.pure $ case m of
        Nothing -> Empty ()
        Just (Ur b) -> Chunk b loop
{-# INLINEABLE reread #-}

-- | Make the information in a bytestring available to more than one eliminating fold, e.g.
--
-- >>>  Q.count 'l' $ Q.count 'o' $ Q.copy $ "hello\nworld"
-- 3 :> (2 :> ())
--
-- >>> Q.length $ Q.count 'l' $ Q.count 'o' $ Q.copy $ Q.copy "hello\nworld"
-- 11 :> (3 :> (2 :> ()))
copy :: (Control.Monad m) => ByteStream m r %1 -> ByteStream (ByteStream m) r
copy = \case
  Empty r -> Empty r
  Go m -> Go . Control.fmap copy $ Control.lift m
  Chunk bs bss -> Chunk bs . Go . Chunk bs . Empty $ copy bss
{-# INLINEABLE copy #-}

-- | 'findIndexOrEnd' is a variant of 'B.findIndex', that returns the
-- length of the string if no element is found, rather than Nothing.
findIndexOrEnd :: (Word8 -> Bool) -> B.ByteString -> Int
findIndexOrEnd k (B.PS x s l) =
  B.accursedUnutterablePerformIO $
    unsafeWithForeignPtr x $
      \f -> go (f `plusPtr` s) 0
  where
    go !ptr !n
      | n >= l = Prelude.pure l
      | otherwise = do
          w <- peek ptr
          if k w
            then Prelude.pure n
            else go (ptr `plusPtr` 1) (n + 1)
{-# INLINEABLE findIndexOrEnd #-}
