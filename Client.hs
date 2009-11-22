module Client where

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import qualified Network.Socket.ByteString as N
import qualified Data.ByteString.Char8 as S
import qualified Protocol as P

openConnection hostname port = do
    addrinfos <- getAddrInfo (Just tcpHints) (Just hostname) (Just port)
    let addr = head addrinfos
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    setSocketOption sock NoDelay 1
    connect sock (addrAddress addr)
    return $ sock
    where tcpHints = defaultHints {addrFamily = AF_INET, addrProtocol = 6}

sendThenReceive sock cmd = withSocketsDo $ N.send sock cmd >> N.recv sock 2048 >>= return

pingDo sock = sendThenReceive sock P.ping >>= \x -> return $ wrapSingle x
setDo sock k v = sendThenReceive sock (P.set k v)
getDo sock s = sendThenReceive sock (P.get s)
getsetDo sock k v = sendThenReceive sock (P.getset k v)
mget sock keys = sendThenReceive sock $ P.mget keys
exists sock key = sendThenReceive sock $ P.exists key
del sock key = sendThenReceive sock $ P.exists key

wrapSingle s = case (S.head s) of
    '-' -> Left $ S.drop 4 s
    '+' -> Right . P.singleLineReply $ S.tail s

wrapInt s = case (S.head s) of
    '-' -> Left $ S.drop 4 s
    ':' -> Right $ P.integerReply s

wrapBulk s = case (S.head s) of
    '-' -> Left $ S.drop 4 s
    '$' -> Right $ P.bulkReply s

wrapSet s = case (S.head s) of
    '-' -> Left $ S.drop 4 s
    '*' -> Right $ P.setReply s

bump = do
    let k = S.pack "key"
    let v = S.pack "value"
    let keys = map S.pack ["k1", "k2", "k3", "k4"]
    s <- openConnection "localhost" "6379"
    sd <- setDo s k v
    setDo s (S.pack "k2") v
    mapM_ (\x -> setDo s x v) keys
    gd <- getDo s k
    print gd
    let mm = P.mget keys
    mget_res <- sendThenReceive s mm
    dbs <- sendThenReceive s P.dbSize
    let pk = S.pack "pusher"
    let lpush_cmd = P.rPush pk (S.pack "pushed")
    pres <- sendThenReceive s lpush_cmd 
    print pres
    llen_res <- sendThenReceive s (P.llen pk)
    print llen_res
    --return dbs
    gs_res <- getsetDo s k (S.pack "NEW")
    print gs_res
    mget_res2 <- mget s keys
    print mget_res2
    b <- exists s k
    del_res <- del s k
    return del_res
    --return $ wrapInt b
    --return $ wrapSet mget_res2
    --return $ wrapBulk gs_res

