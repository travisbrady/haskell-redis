module Protocol where

import qualified Data.List as L
import Network.Socket hiding (send, sendTo, recv, recvFrom)
--import qualified Network.Socket.ByteString.Lazy as N
import qualified Network.Socket.ByteString as N
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as S
import Control.Monad

sp = S.singleton ' '
crlf = S.pack "\r\n"
errString = S.pack "-ERR"
neg = S.pack "$-1\r\n"
bulkResp = S.pack "$6\r\nfoobar\r\n" 
fakeSingleLineResponse = S.pack "+OK\r\n"
fakeIntResponse = S.pack ":1000\r\n"
fakeBulkResponses = S.pack "$6\r\nwayans\r\n$9\r\npushingit\r\n$3\r\nXXX\r\n"
fakeSetRepsonse = S.pack "*4\r\n$-1\r\n$6\r\nwayans\r\n$9\r\npushingit\r\n$3\r\nXXX\r\n"

errorReply = S.drop 4
integerReply :: S.ByteString -> Maybe Int
integerReply s = fmap fst . S.readInt $ S.tail s

singleLineReply :: S.ByteString -> S.ByteString
singleLineReply s = S.take ((S.length s) - 2) $ S.tail s

_bulkReply :: S.ByteString -> (Maybe S.ByteString, S.ByteString)
_bulkReply s = case n of
    Nothing -> (Nothing, S.empty)
    Just (len, rest) -> case len of
        -1 -> (Nothing, S.drop 2 rest)
        _  -> let (x, y) = S.splitAt (toEnum len) $ S.drop 2 rest in (Just x, S.drop 2 y)
    where n = S.readInt $ S.tail s

--"$5\r\nvalue\r\n"
bulkReply :: S.ByteString -> Maybe S.ByteString
bulkReply = fst . _bulkReply

bulkLoop s acc
    | S.null s || s == crlf = reverse acc
    | otherwise = bulkLoop rest (chunk:acc)
        where (chunk, rest) = _bulkReply s

takeRestFrom needle s
    | S.null s = s
    | S.isPrefixOf needle s = S.drop (S.length needle) s
    | otherwise = takeRestFrom needle $ S.tail s

setReply :: S.ByteString -> [Maybe S.ByteString]
setReply s = bulkLoop rest []
    where rest = takeRestFrom crlf s

inlineCmd cmd = S.concat [cmd, crlf]
ping = inlineCmd $ S.pack "PING"
randomKey = inlineCmd $ S.pack "RANDOMKEY"
bgSave = inlineCmd $ S.pack "BGSAVE"
save = inlineCmd $ S.pack "SAVE"
lastSave = inlineCmd $ S.pack "LASTSAVE"
flushAll = inlineCmd $ S.pack "FLUSHALL"
flushDB = inlineCmd $ S.pack "FLUSHDB"
dbSize = inlineCmd $ S.pack "DBSIZE"

oneArg cmd arg = S.concat [S.pack cmd, sp, arg, crlf]
get = oneArg "GET"
mget keys = oneArg "MGET" $ S.concat $ L.intersperse sp keys
exists = oneArg "EXISTS"
del = oneArg "DEL"
getType = oneArg "TYPE"
keys = oneArg "KEYS"
llen = oneArg "LLEN"
rPop = oneArg "RPOP"
lPop = oneArg "LPOP"
sInter args = oneArg "SINTER" $ S.unwords args
sMembers = oneArg "SMEMBERS"
select = oneArg "SELECT"

twoArgs cmd arg1 arg2 = S.concat [S.pack cmd, sp, arg1, sp, arg2, crlf]
incrBy name num = twoArgs "INCRBY" name (S.pack $ show num)
incr name = incrBy name 1
decrBy name num = twoArgs "DECRBY" name (S.pack $ show num)
decr name = decrBy name 1
rename = twoArgs "RENAME"
renamenx = twoArgs "RENAMENX"
move name db = twoArgs "MOVE" name db
sInterStore name args = twoArgs "SINTERSTORE" name $ S.unwords args

sLen = S.pack . show . S.length
setCmds cmd name value = S.concat [S.pack cmd, sp, name, sp, sLen value, crlf, value, crlf]
set = setCmds "SET"
getset = setCmds "GETSET"
setnx = setCmds "SETNX"
lPush = setCmds "LPUSH"
rPush = setCmds "RPUSH"
lSet = setCmds "LSET"
lRem = setCmds "LREM"
sAdd = setCmds "SADD"
sRem = setCmds "SREM"
sIsMember = setCmds "SISMEMBER"

data Order = Asc | Desc
instance Show Order where
    show Asc = "ASC"
    show Desc = "DESC"

data SortOpt = By String
             | Get String
             | Incr String
             | Del String
             | Decr String
             | Limit Int Int
             | Alpha

instance Show SortOpt where
    show (By s) = "BY " ++ s
    show (Get s) = "GET " ++ s
    show (Incr s) = "INCR " ++ s
    show (Del s) = "DEL " ++ s
    show (Decr s) = "DECR " ++ s
    show (Limit start end) = "LIMIT " ++ (show start) ++ " " ++ (show end)
    show Alpha = "ALPHA"
