from ctypes import *
from ctypes.wintypes import VARIANT_BOOL

from comtypes import COMMETHOD, GUID, dispid
from comtypes.automation import VARIANT, IDispatch, _midlSAFEARRAY

from src.core import SETTINGS

# GUIDs
GUID_IRtdServer = GUID(SETTINGS['rtd']['server_guid'])
GUID_IRTDUpdateEvent = GUID(SETTINGS['rtd']['update_event_guid'])
LIBID_RTDServerLib = GUID(SETTINGS['rtd']['typelib_guid'])

# Interfaces
class IRTDUpdateEvent(IDispatch):
    _case_insensitive_ = True
    _iid_ = GUID_IRTDUpdateEvent
    _idlflags_ = ['dual', 'oleautomation']
    _methods_ = [
        COMMETHOD([dispid(10)], HRESULT, 'UpdateNotify'),
        COMMETHOD([dispid(11), 'propget'], HRESULT, 'HeartbeatInterval',
                  (['out', 'retval'], POINTER(c_int), 'plRetVal')),
        COMMETHOD([dispid(11), 'propput'], HRESULT, 'HeartbeatInterval',
                  (['in'], c_int, 'plRetVal')),
        COMMETHOD([dispid(12)], HRESULT, 'Disconnect'),
    ]

class IRtdServer(IDispatch):
    _case_insensitive_ = True
    _iid_ = GUID_IRtdServer
    _idlflags_ = ['dual', 'oleautomation']
    _methods_ = [
        COMMETHOD([dispid(10)], HRESULT, 'ServerStart',
                  (['in'], POINTER(IRTDUpdateEvent), 'CallbackObject'),
                  (['out', 'retval'], POINTER(c_int), 'pfRes')),
        COMMETHOD([dispid(11)], HRESULT, 'ConnectData',
                  (['in'], c_int, 'TopicID'),
                  (['in'], POINTER(_midlSAFEARRAY(VARIANT)), 'Strings'),
                  (['in', 'out'], POINTER(VARIANT_BOOL), 'GetNewValues'),
                  (['out', 'retval'], POINTER(VARIANT), 'pvarOut')),
        COMMETHOD([dispid(12)], HRESULT, 'RefreshData',
                  (['in', 'out'], POINTER(c_int), 'TopicCount'),
                  (['out', 'retval'], POINTER(_midlSAFEARRAY(VARIANT)), 'parrayOut')),
        COMMETHOD([dispid(13)], HRESULT, 'DisconnectData',
                  (['in'], c_int, 'TopicID')),
        COMMETHOD([dispid(14)], HRESULT, 'Heartbeat',
                  (['out', 'retval'], POINTER(c_int), 'pfRes')),
        COMMETHOD([dispid(15)], HRESULT, 'ServerTerminate'),
    ]

class Library(object):
    name = 'RTDServerLib'
    _reg_typelib_ = (LIBID_RTDServerLib, 1, 0)

__all__ = ['IRTDUpdateEvent', 'IRtdServer', 'Library']