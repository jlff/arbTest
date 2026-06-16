# encoding: gbk
# =================================================================
# Test_Yinhe_qmt_ServerV5.py - 银河QMT Socket Server v5.0
# 版本日期：2026-06-15
# 【重要】此文件是运行在银河QMT客户端内部的策略代码
# 不是Python主程序调用的，请在QMT策略编辑器中加载此代码
# =================================================================
# 架构：
#   Socket 子线程 → pending_actions 队列 → tick_push 定时器(200ms)
#   主线程 drain → 执行 passorder（安全，不阻塞消息总线）
#   quote_push 定时器(1s) → 主线程 get_full_tick → 广播 TICK
#
# 外部协议（兼容 v4.0，trade_manager.py 无需修改）：
#   BUY,code,volume,price          → OK\n
#   SELL,code,volume,price         → OK\n
#   QUERY_TICK,code                → TICK_RESULT,code,lastPrice,preClose\n
#   SUBSCRIBE,code1,code2,...      → SUBSCRIBE_OK\n
#   PING                           → PONG\n
#
# 参考：Jiang_big_qmt_trader_server.py v3.6.0
# - builtins 共享状态解决 QMT 命名空间隔离
# - socket_thread_gen 解决僵尸线程残留
# - pending_actions 队列保证 C++ API 只在主线程调用
# =================================================================

import builtins
import socket
import threading
import time

# ==================== 版本 ====================
SERVER_VERSION = '5.0.0 (2026-06-15)'

# ==================== 共享状态（builtins）====================
# QMT 对每个回调（init, handlebar, tick_push, socket线程）赋予独立命名空间
# 模块级全局变量在各命名空间中是不同的对象！
# 只有 builtins 是真正跨命名空间共享的。
_V5_KEY = '_qmt_v5_state'

def _S():
    """获取共享状态字典，首次访问时初始化"""
    s = getattr(builtins, _V5_KEY, None)
    if s is None:
        s = {}
        setattr(builtins, _V5_KEY, s)
    defaults = {
        'context': None,
        'account_id': None,
        'account_type': None,
        'active_clients': [],
        'clients_lock': threading.Lock(),
        'api_lock': threading.Lock(),
        'pending_actions': [],
        'pending_lock': threading.Lock(),
        'subscribed_stocks': set(),
        'latest_ticks': {},           # code -> tick dict（QUERY_TICK 从此读，不调 C++）
        'ticks_lock': threading.Lock(),
        'socket_gen': [0],            # bumped by init(); 旧线程检测到 gen 不匹配时自行退出
        'push_count': [0],
    }
    for k, v in defaults.items():
        if k not in s:
            s[k] = v
    return s

_S()  # 模块加载时确保状态字典存在

# ==================== 队列机制 ====================
def _enqueue(action):
    """安全入队，可在任意线程调用（纯 Python 操作，不碰 C++ API）"""
    s = _S()
    with s['pending_lock']:
        s['pending_actions'].append(action)

def _drain(ContextInfo):
    """在主线程定时器回调中执行：出队并调用 C++ API"""
    s = _S()
    with s['pending_lock']:
        if not s['pending_actions']:
            return
        actions = s['pending_actions'][:]
        s['pending_actions'][:] = []

    for act in actions:
        kind = act.get('kind')
        try:
            if kind == 'place':
                _do_place(ContextInfo, act)
            elif kind == 'cancel':
                _do_cancel(ContextInfo, act)
        except Exception as e:
            print(f"[QMTv5][drain] EXC: {e}")

def _do_place(ContextInfo, act):
    """主线程执行 passorder"""
    s = _S()
    acc_id = s['account_id']
    if not acc_id:
        print("[QMTv5][place] account not ready, skip")
        return
    side = act['side']  # 'BUY' or 'SELL'
    op_type = 23 if side == 'BUY' else 24
    try:
        passorder(op_type, 1101, acc_id, act['code'], 11,
                  act['price'], act['volume'],
                  'QMTv5', 1, '', ContextInfo)
        print(f"[QMTv5][place] OK: {side} {act['code']} {act['volume']}@{act['price']}")
    except Exception as e:
        print(f"[QMTv5][place] FAIL: {e}")

def _do_cancel(ContextInfo, act):
    """主线程执行撤单"""
    s = _S()
    acc_id = s['account_id']
    if not acc_id:
        return
    try:
        ok = cancel(act['sysid'], acc_id, s['account_type'], ContextInfo)
        print(f"[QMTv5][cancel] sysid={act['sysid']} ok={ok}")
    except Exception as e:
        print(f"[QMTv5][cancel] FAIL: {e}")

# ==================== Socket 层 ====================
def _safe_send(conn, data):
    try:
        conn.sendall(data)
    except Exception:
        pass

def _broadcast(msg):
    """向所有活跃客户端广播消息"""
    encoded = msg.encode('utf-8')
    s = _S()
    with s['clients_lock']:
        dead = []
        for c in s['active_clients']:
            try:
                c.sendall(encoded)
            except Exception:
                dead.append(c)
        for c in dead:
            s['active_clients'].remove(c)

def client_handler(conn, addr):
    """Socket 子线程：只做网络 IO，不调 C++ API"""
    s = _S()
    with s['clients_lock']:
        s['active_clients'].append(conn)

    buffer = ''
    try:
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break
            buffer += data
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                line = line.strip()
                if not line:
                    continue

                parts = line.split(',')
                action = parts[0].upper()

                if action == 'PING':
                    _safe_send(conn, b'PONG\n')

                elif action in ('BUY', 'SELL') and len(parts) >= 4:
                    # BUY,code,volume,price  → 入队，主线程执行 passorder
                    _enqueue({
                        'kind': 'place',
                        'side': action,
                        'code': parts[1].strip(),
                        'volume': int(parts[2]),
                        'price': float(parts[3]),
                    })
                    _safe_send(conn, b'OK\n')

                elif action == 'QUERY_TICK' and len(parts) >= 2:
                    # 从缓存读取，不调 C++
                    code = parts[1].strip()
                    with s['ticks_lock']:
                        tick = s['latest_ticks'].get(code, {})
                    last_price = tick.get('lastPrice', 0)
                    pre_close = tick.get('lastClose', 0)
                    resp = f"TICK_RESULT,{code},{last_price},{pre_close}\n"
                    _safe_send(conn, resp.encode('utf-8'))

                elif action == 'SHUTDOWN':
                    print(f"[QMTv5] SHUTDOWN received, signaling old server to exit")
                    _broadcast("SHUTDOWN\n")
                    # 碰 socket_gen 让所有旧线程退出
                    s['socket_gen'][0] += 1

                elif action == 'SUBSCRIBE' and len(parts) > 1:
                    new_codes = [p.strip() for p in parts[1:] if p.strip()]
                    s['subscribed_stocks'].update(new_codes)
                    print(f"[QMTv5] SUBSCRIBE: {new_codes}")
                    _safe_send(conn, b'SUBSCRIBE_OK\n')

                else:
                    _safe_send(conn, b'UNKNOWN\n')

    except Exception:
        pass
    finally:
        with s['clients_lock']:
            if conn in s['active_clients']:
                s['active_clients'].remove(conn)
        try:
            conn.close()
        except Exception:
            pass

def socket_server_thread():
    """Socket 监听线程：支持 generation 自退出 + 僵尸端口抢占"""
    s = _S()
    my_gen = s['socket_gen'][0]

    # ---- 僵尸端口抢占：先通知旧 v5.0 线程退出 ----
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(0.5)
        probe.connect(('127.0.0.1', 8888))
        probe.sendall(b'SHUTDOWN\n')
        probe.close()
        time.sleep(0.5)
        print(f"[QMTv5] sent SHUTDOWN to old server on 8888")
    except Exception:
        pass  # 没有旧服务端在运行，正常启动

    # ---- 绑定端口（SO_REUSEADDR 允许与僵尸共存，但我们是最后绑定者）----
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(('127.0.0.1', 8888))
        server.listen(5)
        server.settimeout(1.0)
        print(f"[QMTv5] Server listening on 8888 (gen={my_gen})")
    except Exception as e:
        print(f"[QMTv5] bind 8888 failed: {e}")
        return

    try:
        while True:
            current_gen = s['socket_gen'][0]
            if current_gen != my_gen:
                print(f"[QMTv5] old server thread exits (gen {my_gen} != {current_gen})")
                break
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            except Exception:
                time.sleep(1)
                continue
            t = threading.Thread(target=client_handler, args=(conn, addr))
            t.daemon = True
            t.start()
    finally:
        try:
            server.close()
        except Exception:
            pass

# ==================== QMT 回调 ====================
def init(ContextInfo):
    print("=" * 50)
    print("[QMTv5] v5.0 主线程队列模式启动")
    print("=" * 50)

    s = _S()
    s['context'] = ContextInfo

    # 从 QMT 全局变量读取账户（"资金账号"绑定）
    try:
        s['account_id'] = account
        s['account_type'] = accountType
        print(f"[QMTv5] account: id={account!r} type={accountType!r}")
    except NameError:
        print("[QMTv5][WARN] account globals not found, place/cancel will fail")
        s['account_id'] = None
        s['account_type'] = None

    if s['account_id']:
        try:
            ContextInfo.set_account(s['account_id'])
        except Exception as e:
            print(f"[QMTv5][WARN] set_account failed: {e}")

    # 生成新 generation，旧 socket 线程将自动退出
    s['socket_gen'][0] += 1
    t = threading.Thread(target=socket_server_thread)
    t.daemon = True
    t.start()

    # 200ms 定时器：消费订单队列（主线程执行 passorder）
    ContextInfo.run_time("tick_push", "200nMilliSecond", "2020-01-01 09:30:00")
    # 1s 定时器：推送行情（主线程执行 get_full_tick）
    ContextInfo.run_time("quote_push", "1nSecond", "2020-01-01 09:30:00")

    print(f"[QMTv5] v{SERVER_VERSION} initialized, account={s['account_id']}")

def tick_push(ContextInfo):
    """200ms 定时器：消费 pending_actions 队列"""
    _drain(ContextInfo)
    s = _S()
    s['push_count'][0] += 1

def quote_push(ContextInfo):
    """1s 定时器：推送 TICK 行情"""
    s = _S()
    if not s['subscribed_stocks'] or not s['account_id']:
        return
    codes = list(s['subscribed_stocks'])
    if not codes:
        return
    with s['api_lock']:  # 主线程调 C++ 也加锁保护（多重定时器防竞争）
        try:
            ticks = ContextInfo.get_full_tick(codes)
        except Exception:
            return
    if not ticks:
        return

    # 更新缓存（供 QUERY_TICK 读取）
    with s['ticks_lock']:
        s['latest_ticks'].update(ticks)

    # 广播 TICK 给所有客户端
    for code, tick in ticks.items():
        ask_prices = tick.get('askPrice', [0]*5)
        ask_vols   = tick.get('askVol',   [0]*5)
        bid_prices = tick.get('bidPrice', [0]*5)
        bid_vols   = tick.get('bidVol',   [0]*5)
        pre_close  = tick.get('lastClose', 0)
        amount     = tick.get('amount', 0)
        msg = (f"TICK,{code},{tick.get('lastPrice', 0)},{tick.get('volume', 0)},"
               f"{ask_prices[0]},{ask_vols[0]},{ask_prices[1]},{ask_vols[1]},"
               f"{ask_prices[2]},{ask_vols[2]},{ask_prices[3]},{ask_vols[3]},"
               f"{ask_prices[4]},{ask_vols[4]},"
               f"{bid_prices[0]},{bid_vols[0]},{bid_prices[1]},{bid_vols[1]},"
               f"{bid_prices[2]},{bid_vols[2]},{bid_prices[3]},{bid_vols[3]},"
               f"{bid_prices[4]},{bid_vols[4]},"
               f"{pre_close},{amount}\n")
        _broadcast(msg)

def handlebar(ContextInfo):
    """QMT 框架要求必须存在，仅用于 GIL 让渡"""
    time.sleep(0.001)

def order_callback(ContextInfo, orderInfo):
    """委托状态更新"""
    try:
        status = getattr(orderInfo, 'm_nOrderStatus', None)
        sysid = getattr(orderInfo, 'm_strOrderSysID', '') or ''
        code = getattr(orderInfo, 'm_strInstrumentID', '') or ''
        print(f"[QMTv5][order] code={code} sysid={sysid} status={status}")
    except Exception:
        pass

def deal_callback(ContextInfo, dealInfo):
    """成交回报"""
    try:
        code = getattr(dealInfo, 'm_strInstrumentID', '')
        price = getattr(dealInfo, 'm_dPrice', 0.0)
        vol = getattr(dealInfo, 'm_nVolume', 0)
        print(f"[QMTv5][deal] {code} {vol}@{price}")
    except Exception:
        pass

def orderError_callback(ContextInfo, passOrderInfo, msg):
    """下单错误"""
    try:
        code = getattr(passOrderInfo, 'm_strInstrumentID', '') or ''
        print(f"[QMTv5][error] code={code} msg={msg}")
    except Exception:
        pass

def position_callback(ContextInfo, positionInfo):
    """持仓更新"""
    try:
        code = getattr(positionInfo, 'm_strInstrumentID', '') or ''
        vol = getattr(positionInfo, 'm_nVolume', 0) or 0
        price = getattr(positionInfo, 'm_dOpenPrice', 0.0) or 0.0
        if vol > 0:
            print(f"[QMTv5][position] {code} {vol}@{price}")
    except Exception:
        pass

_last_cash_key = '_qmt_v5_last_cash'

def account_callback(ContextInfo, accountInfo):
    """资金更新（仅变化时打印，避免每5秒刷屏）"""
    try:
        last = getattr(builtins, _last_cash_key, 0.0)
        avail = getattr(accountInfo, 'm_dAvailable', 0.0) or 0.0
        if abs(avail - last) > 0.01:
            setattr(builtins, _last_cash_key, avail)
            print(f"[QMTv5][cash] available={avail}")
    except Exception:
        pass
