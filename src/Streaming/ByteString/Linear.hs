{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE LinearTypes #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Streaming.ByteString.Linear
  ( -- * The @ByteStream@ type
    ByteStream (..),

    -- * Introducing and eliminating 'ByteStream's
    empty,
    singleton,
    pack,
    unpack,
    fromLazy,
    toLazy,
    toLazy_,
    fromChunks,
    toChunks,
    fromStrict,
    toStrict,
    toStrict_,
    effects,
    copy,
    drained,
    mwrap,

    -- * Transforming 'ByteStream's
    map,
    for,
    intercalate,
    intersperse,

    -- * Basic interface
    cons,
    cons',
    snoc,
    append,
    filter,
    uncons,

    -- * Substrings

    -- ** Breaking strings
    break,
    drop,
    dropWhile,
    group,
    groupBy,
    span,
    splitAt,
    splitWith,
    take,
    takeWhile,

    -- ** Breaking into many substrings
    split,

    -- ** Special folds
    concat,
    denull,

    -- * Builders
    toStreamingByteString,
    toStreamingByteStringWith,
    toBuilder,
    concatBuilders,

    -- * Building 'ByteStreams'

    -- ** Infinite 'ByteStreams'
    repeat,
    iterate,
    cycle,

    -- ** Unfolding 'ByteStreams'
    unfoldM,
    unfoldr,
    reread,

    -- ** Folds, including support for @Control.Foldl@
    foldr,
    fold,
    fold_,
    head,
    head_,
    last,
    last_,
    length,
    length_,
    null,
    null_,
    nulls,
    testNull,
    count,
    count_,

    -- * I\/O with 'ByteStream's

    -- ** Standard input and output
    getContents,
    stdin,
    stdout,
    interact,

    -- ** Files
    readFile,
    writeFile,
    appendFile,

    -- ** I\/O with 'Handle's
    fromHandle,
    toHandle,
    hGet,
    hGetContents,
    hGetContentsN,
    hGetN,
    hPut,
    -- hPutNonBlocking,

    -- * Simple chunkwise operations
    unconsChunk,
    nextChunk,
    chunk,
    foldrChunks,
    foldlChunks,
    chunkFold,
    chunkFoldM,
    chunkMap,
    chunkMapM,
    chunkMapM_,

    -- * Etc.
    hoist,
    dematerialize,
    materialize,
    distribute,
    zipWithStream,
  )
where

import qualified Control.Functor.Linear as Control
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import Data.ByteString.Lazy (LazyByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Internal as BLI
import qualified Data.ByteString.Unsafe as B
import qualified Data.Functor.Linear as Data
import Data.Int (Int64)
import qualified Data.List as NonLinear
import Data.Word (Word8)
import Foreign.Ptr (plusPtr)
import Foreign.Storable (poke)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Prelude.Linear hiding
  ( drop,
    dropWhile,
    filter,
    group,
    intercalate,
    intersperse,
    map,
    readFile,
    uncons,
  )
import Streaming.ByteString.Linear.Internal
import Streaming.Linear (Of (..), Stream (..), destroy)
import qualified Streaming.Prelude.Linear as S
import System.IO.Resource.Linear

empty :: ByteStream m ()
empty = Empty ()

singleton :: Word8 -> ByteStream m ()
singleton w = Chunk (B.singleton w) empty

-- | Convert a `Stream` of pure `Word8` into a chunked 'ByteStream'.
pack :: (Control.Monad m) => Stream (Of Word8) m r %1 -> ByteStream m r
pack cs0 = Control.do
  -- XXX: Why 32?  It seems like a rather small chunk size, wouldn't
  -- smallChunkSize make a better choice?
  bytes :> rest <- Control.lift $ S.toList $ S.splitAt 32 cs0
  case bytes of
    [] -> case rest of
      Return r -> Empty r
      Step as -> pack (Step as) -- these two pattern matches
      Effect m -> Go $ Data.fmap pack m -- should be avoided.
    _ -> Chunk (B.pack bytes) (pack rest)
{-# INLINEABLE pack #-}

unpack :: (Control.Monad m) => ByteStream m r %1 -> Stream (Of Word8) m r
unpack = \case
  Empty r -> Return r
  Chunk bs bss -> S.each' (B.unpack bs) Control.>> unpack bss
  Go m -> Effect $ Data.fmap unpack m
{-# INLINEABLE unpack #-}

fromLazy :: BL.ByteString -> ByteStream m ()
fromLazy bs0 = NonLinear.foldr Chunk (Empty ()) $ BL.toChunks bs0

toLazy :: (Control.Monad m) => ByteStream m r %1 -> m (Of LazyByteString r)
toLazy bs0 =
  dematerialize
    bs0
    (\r -> Control.pure (BL.empty :> r))
    (\b -> Control.fmap $ \(bs :> r) -> BLI.Chunk b bs :> r)
    Control.join

toLazy_ ::
  (Control.Monad m, Consumable r) => ByteStream m r %1 -> m LazyByteString
toLazy_ bs =
  dematerialize
    bs
    ( \r -> case consume r of
        () -> Control.pure BL.empty
    )
    (Control.fmap . BLI.Chunk)
    Control.join

fromChunks :: (Control.Monad m) => Stream (Of ByteString) m r %1 -> ByteStream m r
fromChunks cs = destroy cs (\(bs :> rest) -> Chunk bs rest) Go Empty

toChunks :: (Control.Monad m) => ByteStream m r %1 -> Stream (Of ByteString) m r
toChunks bs = dematerialize bs Control.pure (\b rest -> Step (b :> rest)) Effect

fromStrict :: ByteString -> ByteStream m ()
fromStrict bs = Chunk bs $ Empty ()

toStrict :: (Control.Monad m) => ByteStream m r %1 -> m (Of ByteString r)
toStrict bs = Control.do
  bss :> r <- S.toList (toChunks bs)
  Control.pure (B.concat bss :> r)

toStrict_ ::
  (Control.Monad m, Consumable r) => ByteStream m r %1 -> m ByteString
toStrict_ bs = Control.do
  b :> r <- toStrict bs
  case consume r of
    () -> Control.pure b

effects :: (Control.Monad m) => ByteStream m r %1 -> m r
effects = \case
  Empty r -> Control.pure r
  Chunk _ bss -> effects bss
  Go m -> m Control.>>= effects

drained ::
  (Control.Monad m, Control.MonadTrans t) => t m (ByteStream m r) %1 -> t m r
drained t = t Control.>>= Control.lift . effects

map ::
  (Control.Monad m) => (Word8 -> Word8) -> ByteStream m r %1 -> ByteStream m r
map f = \case
  Empty r -> Empty r
  Chunk bs bss -> Chunk (B.map f bs) $ map f bss
  Go m -> Go $ Control.fmap (map f) m

for ::
  (Control.Monad m) =>
  ByteStream m r %1 ->
  (ByteString %1 -> ByteStream m ()) ->
  ByteStream m r
for bss0 f = case bss0 of
  Empty r -> Empty r
  Chunk bs bss -> f bs Control.>> for bss f
  Go m -> Go $ Control.fmap (`for` f) m

intercalate ::
  (Control.Monad m) =>
  ByteStream m () ->
  Stream (ByteStream m) m r %1 ->
  ByteStream m r
intercalate bss = \case
  Return r -> Empty r
  Effect m -> Go $ Control.fmap (intercalate bss) m
  Step f ->
    f Control.>>= \case
      Return r -> Empty r
      bs -> bss Control.>> intercalate bss bs

intersperse :: (Control.Monad m) => Word8 -> ByteStream m r %1 -> ByteStream m r
intersperse w = \case
  Empty r -> Empty r
  Go m -> Go $ Control.fmap (intersperse w) m
  Chunk bs bss
    | B.null bs -> intersperse w bss
    | otherwise ->
        Chunk
          (B.intersperse w bs)
          (dematerialize bss Empty (\b -> Chunk (intersperse' b)) Go)
  where
    intersperse' :: ByteString -> ByteString
    intersperse' (B.BS fp len)
      | len == 0 = B.empty
      | otherwise = B.unsafeCreate (2 * len) $ \p' ->
          unsafeWithForeignPtr fp $ \p -> do
            poke p' w
            B.c_intersperse (p' `plusPtr` 1) p (fromIntegral len) w
{-# INLINEABLE intersperse #-}

-- | @'repeat' x@ is an infinite ByteStream, with @x@ the value of every
--     element.
--
-- >>> R.stdout $ R.take 50 $ R.repeat 60
-- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
-- >>> Q.putStrLn $ Q.take 50 $ Q.repeat 'z'
-- zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz
repeat :: Word8 -> ByteStream m r
repeat w = cs where cs = Chunk (B.replicate smallChunkSize w) cs
{-# INLINEABLE repeat #-}

cons :: Word8 -> ByteStream m r %1 -> ByteStream m r
cons b bs = Chunk (B.singleton b) bs
{-# INLINEABLE cons #-}

cons' :: Word8 -> ByteStream m r %1 -> ByteStream m r
cons' b (Chunk c cs) | B.length c < 16 = Chunk (B.cons b c) cs
cons' b cs = Chunk (B.singleton b) cs
{-# INLINEABLE cons' #-}

snoc :: (Control.Monad m) => ByteStream m r %1 -> Word8 -> ByteStream m r
snoc bs b = Control.do
  r <- bs
  singleton b
  Control.pure r
{-# INLINEABLE snoc #-}

uncons ::
  (Control.Monad m) => ByteStream m r %1 -> m (Either r (Word8, ByteStream m r))
uncons (Chunk c@(B.length -> len) cs)
  | len > 0 =
      let !h = B.unsafeHead c
       in -- Gross, but GHC doesn't think cs is consumed if we write
          -- let !t = if len > 1 then Chunk (B.unsafeTail c) cs else cs
          case if len > 1 then Chunk (B.unsafeTail c) cs else cs of
            t -> Control.pure (Right (h, t))
  | otherwise = uncons cs
uncons (Go m) = m Control.>>= uncons
uncons (Empty r) = Control.pure (Left r)
{-# INLINEABLE uncons #-}

break ::
  (Control.Monad m) =>
  (Word8 -> Bool) ->
  ByteStream m r %1 ->
  ByteStream m (ByteStream m r)
break f cs0 = break' cs0
  where
    break' (Empty r) = Empty (Empty r)
    break' (Chunk c cs) =
      case findIndexOrEnd f c of
        0 -> Empty (Chunk c cs)
        n
          | n < B.length c ->
              Chunk (B.take n c) $
                Empty (Chunk (B.drop n c) cs)
          | otherwise -> Chunk c (break' cs)
    break' (Go m) = Go (Data.fmap break' m)
{-# INLINEABLE break #-}

drop ::
  (Control.Monad m) =>
  Int64 ->
  ByteStream m r %1 ->
  ByteStream m r
drop i p | i <= 0 = p
drop i cs0 = drop' i cs0
  where
    drop' 0 cs = cs
    drop' _ (Empty r) = Empty r
    drop' n (Chunk c cs) =
      if n < fromIntegral (B.length c)
        then Chunk (B.drop (fromIntegral n) c) cs
        else drop' (n - fromIntegral (B.length c)) cs
    drop' n (Go m) = Go (Data.fmap (drop' n) m)
{-# INLINEABLE drop #-}

dropWhile ::
  (Control.Monad m) =>
  (Word8 -> Bool) ->
  ByteStream m r %1 ->
  ByteStream m r
dropWhile p = drop'
  where
    drop' bs = case bs of
      Empty r -> Empty r
      Go m -> Go (Data.fmap drop' m)
      Chunk c cs -> case findIndexOrEnd (\c -> not (p c)) c of
        0 -> Chunk c cs
        n
          | n < B.length c -> Chunk (B.drop n c) cs
          | otherwise -> drop' cs
{-# INLINEABLE dropWhile #-}

group ::
  (Control.Monad m) =>
  ByteStream m r %1 ->
  Stream (ByteStream m) m r
group = go
  where
    go (Empty r) = Return r
    go (Go m) = Effect (Data.fmap go m)
    go (Chunk c cs)
      | B.length c == 1 = Step (to [c] (B.unsafeHead c) cs)
      | otherwise =
          Step
            (to [B.unsafeTake 1 c] (B.unsafeHead c) (Chunk (B.unsafeTail c) cs))

    to acc !_ (Empty r) = revNonEmptyChunks acc (Empty (Return r))
    to acc !w (Go m) = Go (Data.fmap (to acc w) m)
    to acc !w (Chunk c cs) = case findIndexOrEnd (\x -> x /= w) c of
      0 -> revNonEmptyChunks acc (Empty (go (Chunk c cs)))
      n
        | n == B.length c -> to (B.unsafeTake n c : acc) w cs
        | otherwise ->
            revNonEmptyChunks
              (B.unsafeTake n c : acc)
              (Empty (go (Chunk (B.unsafeDrop n c) cs)))
{-# INLINEABLE group #-}

groupBy ::
  forall m r.
  (Control.Monad m) =>
  (Word8 -> Word8 -> Bool) ->
  ByteStream m r %1 ->
  Stream (ByteStream m) m r
groupBy rel = go
  where
    go :: ByteStream m r %1 -> Stream (ByteStream m) m r
    go (Empty r) = Return r
    go (Go m) = Effect (Data.fmap go m)
    go (Chunk c cs)
      | B.length c == 1 = Step (to [c] (B.unsafeHead c) cs)
      | otherwise =
          Step
            (to [B.unsafeTake 1 c] (B.unsafeHead c) (Chunk (B.unsafeTail c) cs))

    to acc !_ (Empty r) = revNonEmptyChunks acc (Empty (Return r))
    to acc !w (Go m) = Go (Data.fmap (to acc w) m)
    to acc !w (Chunk c cs) = case findIndexOrEnd (\x -> not (rel w x)) c of
      0 -> revNonEmptyChunks acc (Empty (go (Chunk c cs)))
      n
        | n == B.length c -> to (B.unsafeTake n c : acc) w cs
        | otherwise ->
            revNonEmptyChunks
              (B.unsafeTake n c : acc)
              (Empty (go (Chunk (B.unsafeDrop n c) cs)))

append ::
  (Control.Monad m, Consumable r) =>
  ByteStream m r ->
  ByteStream m s ->
  ByteStream m s
append b1 b2 = Control.do
  r <- b1
  case consume r of
    () -> b2

{-# INLINEABLE filter #-}
filter :: (Control.Monad m) => (Word8 -> Bool) -> ByteStream m r %1 -> ByteStream m r
filter p = go
  where
    go (Empty r) = Empty r
    go (Chunk b bs) = consChunk (B.filter p b) (filter p bs)
    go (Go m) = Go (Control.fmap go m)

readFile :: FilePath -> ByteStream RIO ()
readFile fp = Control.lift (openBinaryFile fp ReadMode) Control.>>= hGetContents

hGet :: Handle %1 -> Int -> ByteStream RIO Handle
hGet = hGetN defaultChunkSize

hGetContents :: Handle %1 -> ByteStream RIO ()
hGetContents = hGetContentsN defaultChunkSize

hGetContentsN :: Int -> Handle %1 -> ByteStream RIO ()
hGetContentsN k h = Control.do
  (Ur c, h') <- Control.lift $ unsafeFromSystemIOResource (flip B.hGetSome k) h
  if B.null c
    then Control.lift (hClose h') Control.>> Empty ()
    else Chunk c $ hGetContentsN k h'

hGetN :: Int -> Handle %1 -> Int -> ByteStream RIO Handle
hGetN k handle count
  | count > 0 = readChunks handle count
  -- Differs from streaming-bytestring behaviour, but we must return
  -- the handle in order to typecheck.
  | otherwise = Empty handle
  where
    readChunks :: Handle %1 -> Int -> ByteStream RIO Handle
    readChunks h !i = Go $ Control.do
      (Ur c, h') <- unsafeFromSystemIOResource (flip B.hGet (min k i)) h
      Control.pure $ case B.length c of
        0 -> Empty h'
        m -> Chunk c $ readChunks h' (i - m)

hPut :: Handle %1 -> ByteStream RIO r %1 -> RIO (r, Handle)
hPut h = \case
  Empty r -> Control.pure (r, h)
  Chunk bs bss -> Control.do
    h' <- unsafeFromSystemIOResource_ (flip B.hPut bs) h
    hPut h' bss
  Go m -> m Control.>>= hPut h

-- -- ---------------------------------------------------------------------
-- -- Internal utilities

-- | Used in `group` and `groupBy`.
revNonEmptyChunks :: [B.ByteString] -> ByteStream m r %1 -> ByteStream m r
revNonEmptyChunks = NonLinear.foldl' (\f bs -> Chunk bs . f) id
{-# INLINE revNonEmptyChunks #-}
