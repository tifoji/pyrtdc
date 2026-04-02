# pyrtdc — Python Real-Time Data Client for ThinkOrSwim

A pure Python COM RTD client that receives live market data from ThinkOrSwim's RTD server with **native callback support** — the same mechanism used by C and C# implementations, achieved entirely in Python through comtypes.

![RTD Client Screenshot](rtd.PNG)

## How It Works

### The RTD Protocol

Microsoft's <a href="https://learn.microsoft.com/en-us/previous-versions/office/troubleshoot/office-developer/create-realtimedata-server-in-excel#more-information" target="_blank">Real-Time Data (RTD) protocol</a> is a COM-based mechanism for streaming live data, originally designed for Excel. The protocol defines two interfaces:

- **IRtdServer** (`{EC0E6191-DB51-11D3-8F3E-00C04F3651B8}`) — implemented by the data provider (TOS). Exposes `ServerStart`, `ConnectData`, `RefreshData`, `DisconnectData`, `Heartbeat`, and `ServerTerminate`.
- **IRTDUpdateEvent** (`{A43788C1-D91B-11D3-8F39-00C04F3651B8}`) — implemented by the client (us). The server calls `UpdateNotify` on this interface when new data is available.

The lifecycle is straightforward: the client passes its `IRTDUpdateEvent` callback to `ServerStart`, subscribes to topics via `ConnectData`, and the server calls `UpdateNotify` whenever data changes. The client then calls `RefreshData` to retrieve a 2D array of `[topic_id, value]` pairs.

### The Callback Problem

The critical challenge in Python is **how `UpdateNotify` gets delivered**. The RTD server calls `UpdateNotify` through the **native COM vtable** (slot 7 on `IRTDUpdateEvent`), not through `IDispatch::Invoke`. This means:

- **win32com** (`pythoncom.WrapObject`) — only creates `IDispatch` wrappers. When the server calls `QueryInterface(IID_IRTDUpdateEvent)`, it gets `E_NOINTERFACE`. The callback **never fires**. You're forced to poll `RefreshData` on a timer.
- **comtypes** (`COMObject`) — builds a **real C-level vtable** at runtime via ctypes. When the server queries for `IRTDUpdateEvent`, it gets a proper interface pointer. When it calls vtable slot 7, our Python `UpdateNotify` method executes. **True native callback.**

This is why pyrtdc uses comtypes — it's the only pure Python path to native COM callbacks.

### Reverse-Engineering the Interfaces

The interface definitions in `src/rtd/interfaces.py` were derived by dumping the TOS RTD type library using comtypes:

```python
from comtypes.client import GetModule
# Dump the TOS RTD type library — generates Python bindings in comtypes.gen
GetModule(('{BA792DC8-807E-43E3-B484-47465D82C4D1}', 1, 0))
```

This auto-generates raw Python interface definitions with all GUIDs, dispatch IDs, and method signatures from the registered COM type library. The output was then hand-cleaned into the compact interface definitions used by the client:

```python
class IRTDUpdateEvent(IDispatch):
    _iid_ = GUID('{A43788C1-D91B-11D3-8F39-00C04F3651B8}')
    _idlflags_ = ['dual', 'oleautomation']
    _methods_ = [
        COMMETHOD([dispid(10)], HRESULT, 'UpdateNotify'),
        COMMETHOD([dispid(11), 'propget'], HRESULT, 'HeartbeatInterval', ...),
        COMMETHOD([dispid(11), 'propput'], HRESULT, 'HeartbeatInterval', ...),
        COMMETHOD([dispid(12)], HRESULT, 'Disconnect'),
    ]
```

By declaring `_com_interfaces_ = [IRTDUpdateEvent]` on our `RTDClient(COMObject)`, comtypes generates the native vtable stubs that make the callback work.

### The Event Loop

The main loop uses `MsgWaitForMultipleObjects` — an OS-level efficient wait that blocks the thread at the kernel until a COM message arrives:

```python
win32event.MsgWaitForMultipleObjects([], False, timeout_ms, QS_ALLINPUT)
pythoncom.PumpWaitingMessages()
```

This is necessary because COM callbacks in an STA (Single-Threaded Apartment) are delivered through the Windows message queue. The thread must pump messages for `UpdateNotify` to fire. Unlike a naive `sleep()` + `PumpWaitingMessages()` loop (which creates blind spots where no callbacks are delivered), `MsgWaitForMultipleObjects` wakes **instantly** when data arrives and uses **zero CPU** while idle.

When `UpdateNotify` fires, it calls `RefreshData` inline for immediate data processing. The main loop handles periodic housekeeping (heartbeat checks, summary display) via the timeout fallback.

### STA and Message Pumping

ThinkOrSwim's RTD server operates in the COM Single-Threaded Apartment model. All calls into our callback object are marshaled through the Windows message queue to ensure they execute on the client's thread. This is why:

1. `pythoncom.CoInitialize()` is called at startup (enters STA)
2. Messages must be pumped continuously (`PumpWaitingMessages`)
3. The event loop must run on the same thread that initialized COM

## Installation

```
pip install -r requirements.txt
```

## Usage

```
python main.py
```

ThinkOrSwim must be running to receive updates. Some installations require TOS to be started as admin to register the RTD COM interfaces in the Windows registry.

Edit `config/config.yaml` to adjust timing, logging, and subscription parameters. Set `file_level` to `DEBUG` for detailed output, `INFO` for production.

## Auto-Reconnect

The client automatically detects and recovers from three failure modes:

1. **Server disconnect** — TOS calls `IRTDUpdateEvent.Disconnect()` when it exits. The client detects this instantly via the `disconnected` event and reconnects.

2. **Heartbeat failure** — The periodic `Heartbeat()` call returns unhealthy. The client triggers reconnect instead of just logging a warning.

3. **Zombie detection** — The most subtle failure: TOS restarts but the old COM connection appears alive (heartbeat passes). The client tracks `_last_data_time` and if no actual data arrives for 60 seconds despite a healthy heartbeat, it declares the connection zombie and reconnects.

On reconnect, the client snapshots all active subscriptions, tears down the COM connection, re-initializes, and restores every topic. The main loop resets all timers so housekeeping resumes cleanly.

## Configuration

Key timing settings in `config.yaml`:

| Setting | Default | Description |
|---------|---------|-------------|
| `initial_heartbeat` | 200ms | Initial heartbeat interval (server getter) |
| `default_heartbeat` | 15000ms | Operational heartbeat — MS RTD spec minimum |
| `heartbeat_check_interval` | 30s | How often to verify server health |
| `data_stale_sec` | 60s | No-data threshold for zombie detection |
| `reconnect_delay` | 5s | Pause before reconnect attempt |
| `loop_sleep_time` | 2s | MsgWait timeout (housekeeping interval) |
| `summary_interval` | 30s | Display summary table interval |

## Alternative Approaches

`comtypes` is the recommended approach for native callbacks, but other libraries work for polling-based clients:

- **<a href="https://github.com/tifoji/pyrtdc/wiki/win32com-example" target="_blank">win32com</a>** — simpler syntax via `pywin32`, but limited to `IDispatch` (no native callback). Works well with polling or timer-based refresh.
- **ctypes** — lowest-level option, requires manual vtable construction similar to C.

## Useful Reading

- <a href="https://learn.microsoft.com/en-us/previous-versions/office/troubleshoot/office-developer/create-realtimedata-server-in-excel#more-information" target="_blank">Create a RealTimeData Server in Excel</a> — Microsoft's original documentation
- <a href="https://weblogs.asp.net/kennykerr/Rtd3/" target="_blank">Excel RTD Servers: Minimal C# Implementation</a> — Kenny Kerr's 2008 article on RTD server/client patterns
- <a href="https://github.com/SublimeText/Pywin32/blob/master/lib/x32/win32com/demos/excelRTDServer.py" target="_blank">Excel RTD Server (pywin32 demo)</a> — Reference win32com RTD implementation

## Applications Built on pyrtdc

Contributions and development are encouraged. If you've built something on pyrtdc, please open a PR.

- <a href="https://github.com/2187Nick/tos-streamlit-dashboard/" target="_blank">Tos Streamlit Dashboard</a>
- <a href="https://github.com/2187Nick/tos-market-depth-rtd/tree/main" target="_blank">Tos Market Depth RTD</a>

## License

This project is licensed under the MIT License
