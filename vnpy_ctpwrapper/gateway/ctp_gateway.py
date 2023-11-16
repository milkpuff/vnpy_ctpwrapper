import sys
from datetime import datetime
from time import sleep
from typing import Any, Dict, List, Tuple
from pathlib import Path

from vnpy.event import EventEngine
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
)
from vnpy.trader.utility import get_folder_path, ZoneInfo
from vnpy.trader.event import EVENT_TIMER

from ctpwrapper.ApiStructure import (
    RspInfoField,
    RspUserLoginField,
    SettlementInfoConfirmField,
    InputOrderField,
    InputOrderActionField,
    QryInstrumentField,
    InvestorPositionField,
    QryInvestorPositionField,
    TradingAccountField,
    InstrumentField,
    OrderField,
    TradeField,
    ReqAuthenticateField,
    ReqUserLoginField,
    QryTradingAccountField,
    SpecificInstrumentField,
    DepthMarketDataField,
)
from ctpwrapper.Md import MdApiPy
from ctpwrapper.Trader import TraderApiPy
from .ctp_constant import (
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_OST_Unknown,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete,
    THOST_FTDC_BZTP_Future,

)


# 委托状态映射
STATUS_CTP2VT: Dict[str, Status] = {
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED,
    THOST_FTDC_OST_Unknown: Status.SUBMITTING
}

# 多空方向映射
DIRECTION_VT2CTP: Dict[Direction, str] = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_CTP2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2CTP.items()}
DIRECTION_CTP2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_CTP2VT[THOST_FTDC_PD_Short] = Direction.SHORT

# 委托类型映射
ORDERTYPE_VT2CTP: Dict[OrderType, tuple] = {
    OrderType.LIMIT: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.MARKET: (THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.FAK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV),
    OrderType.FOK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV),
}
ORDERTYPE_CTP2VT: Dict[Tuple, OrderType] = {v: k for k, v in ORDERTYPE_VT2CTP.items()}

# 开平方向映射
OFFSET_VT2CTP: Dict[Offset, str] = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_CTP2VT: Dict[str, Offset] = {v: k for k, v in OFFSET_VT2CTP.items()}

# 交易所映射
EXCHANGE_CTP2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFEX": Exchange.GFEX
}

# 产品类型映射
PRODUCT_CTP2VT: Dict[str, Product] = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

# 期权类型映射
OPTIONTYPE_CTP2VT: Dict[str, OptionType] = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

# 其他常量
MAX_FLOAT = sys.float_info.max                  # 浮点数极限值
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class CtpGateway(BaseGateway):
    """
    VeighNa用于对接期货CTP柜台的交易接口。
    """

    default_name: str = "CTP"

    default_setting: Dict[str, str] = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": ""
    }

    exchanges: List[Exchange] = list(EXCHANGE_CTP2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.td_api: "CtpTdApi" = CtpTdApi(self)
        self.md_api: "CtpMdApi" = CtpMdApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["用户名"]
        password: str = setting["密码"]
        brokerid: str = setting["经纪商代码"]
        td_address: str = setting["交易服务器"]
        md_address: str = setting["行情服务器"]
        appid: str = setting["产品名称"]
        auth_code: str = setting["授权编码"]

        if (
            (not td_address.startswith("tcp://"))
            and (not td_address.startswith("ssl://"))
            and (not td_address.startswith("socks"))
        ):
            td_address = "tcp://" + td_address

        if (
            (not md_address.startswith("tcp://"))
            and (not md_address.startswith("ssl://"))
            and (not md_address.startswith("socks"))
        ):
            md_address = "tcp://" + md_address

        self.td_api.connect(td_address, userid, password,
                            brokerid, auth_code, appid)
        self.md_api.connect(md_address, userid, password, brokerid)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, pRspInfo: RspInfoField) -> None:
        """输出错误信息日志"""
        error_id: int = pRspInfo.ErrorID
        error_msg: str = pRspInfo.ErrorMsg
        self.write_log(f"{msg}，代码：{error_id}，信息：{error_msg}")

    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

        self.md_api.update_date()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.count: int = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class CtpMdApi(MdApiPy):
    """"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: set = set()

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def OnFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def OnFrontDisconnected(self, nReason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{nReason}")

    def OnRspUserLogin(self, pRspUserLogin: RspUserLoginField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """用户登录请求回报"""
        # self.gateway.write_log(f'onRspUserLogin, TradingDay: {pRspUserLogin.TradingDay}, data: {pRspUserLogin}')
        if not pRspInfo.ErrorID:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")
            pInstrumentID = list(self.subscribed)
            self.SubscribeMarketData(pInstrumentID)
        else:
            self.gateway.write_error("行情服务器登录失败", pRspInfo)

    def OnRspError(self, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """请求报错回报"""
        self.gateway.write_error("行情接口报错", pRspInfo)

    def OnRspSubMarketData(self, pSpecificInstrument: SpecificInstrumentField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """订阅行情回报"""
        if not pRspInfo.ErrorID:
            return
        self.gateway.write_error("行情订阅失败", pRspInfo)

    def OnRtnDepthMarketData(self, pDepthMarketData: DepthMarketDataField) -> None:
        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not pDepthMarketData.UpdateTime:
            return

        # 过滤还没有收到合约数据前的行情推送
        symbol: str = pDepthMarketData.InstrumentID
        contract: ContractData | None = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        # 对大商所的交易日字段取本地日期
        if not pDepthMarketData.ActionDay or contract.exchange == Exchange.DCE:
            date_str: str = self.current_date
        else:
            date_str: str = pDepthMarketData.ActionDay

        timestamp: str = f"{date_str} {pDepthMarketData.UpdateTime}.{pDepthMarketData.UpdateMillisec}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=pDepthMarketData.Volume,
            turnover=pDepthMarketData.Turnover,
            open_interest=pDepthMarketData.OpenInterest,
            last_price=adjust_price(pDepthMarketData.LastPrice),
            limit_up=pDepthMarketData.UpperLimitPrice,
            limit_down=pDepthMarketData.LowerLimitPrice,
            open_price=adjust_price(pDepthMarketData.OpenPrice),
            high_price=adjust_price(pDepthMarketData.HighestPrice),
            low_price=adjust_price(pDepthMarketData.LowestPrice),
            pre_close=adjust_price(pDepthMarketData.PreClosePrice),
            bid_price_1=adjust_price(pDepthMarketData.BidPrice1),
            ask_price_1=adjust_price(pDepthMarketData.AskPrice1),
            bid_volume_1=pDepthMarketData.BidVolume1,
            ask_volume_1=pDepthMarketData.AskVolume1,
            gateway_name=self.gateway_name
        )

        if pDepthMarketData.BidVolume2 or pDepthMarketData.AskVolume2:
            tick.bid_price_2 = adjust_price(pDepthMarketData.BidPrice2)
            tick.bid_price_3 = adjust_price(pDepthMarketData.BidPrice3)
            tick.bid_price_4 = adjust_price(pDepthMarketData.BidPrice4)
            tick.bid_price_5 = adjust_price(pDepthMarketData.BidPrice5)

            tick.ask_price_2 = adjust_price(pDepthMarketData.AskPrice2)
            tick.ask_price_3 = adjust_price(pDepthMarketData.AskPrice3)
            tick.ask_price_4 = adjust_price(pDepthMarketData.AskPrice4)
            tick.ask_price_5 = adjust_price(pDepthMarketData.AskPrice5)

            tick.bid_volume_2 = pDepthMarketData.BidVolume2
            tick.bid_volume_3 = pDepthMarketData.BidVolume3
            tick.bid_volume_4 = pDepthMarketData.BidVolume4
            tick.bid_volume_5 = pDepthMarketData.BidVolume5

            tick.ask_volume_2 = pDepthMarketData.AskVolume2
            tick.ask_volume_3 = pDepthMarketData.AskVolume3
            tick.ask_volume_4 = pDepthMarketData.AskVolume4
            tick.ask_volume_5 = pDepthMarketData.AskVolume5

        self.gateway.on_tick(tick)

    def connect(self, address: str, userid: str, password: str, brokerid: str) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.Create((str(path) + "\\Md"))

            self.RegisterFront(pszFrontAddress=address)
            self.Init()

            self.connect_status = True

    def login(self) -> None:
        """用户登录"""
        pReqUserLogin = ReqUserLoginField(
            BrokerID=self.brokerid,
            UserID=self.userid,
            Password=self.password,
        )

        self.reqid += 1
        self.ReqUserLogin(pReqUserLogin, self.reqid)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if self.login_status:
            self.SubscribeMarketData([req.symbol])
        self.subscribed.add(req.symbol)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.gateway.write_log('CtpMdApi close. ')

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")


class CtpTdApi(TraderApiPy):
    """"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0
        self.order_ref: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_status: bool = False
        self.login_failed: bool = False
        self.auth_failed: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""
        self.auth_code: str = ""
        self.appid: str = ""

        self.frontid: int = 0
        self.sessionid: int = 0
        self.order_data: List[OrderField] = []
        self.trade_data: List[TradeField] = []
        self.positions: Dict[str, PositionData] = {}
        self.sysid_orderid_map: Dict[str, str] = {}
        self.active_orders: dict[str, OrderData] = {}  # {orderid: order}
        # {sysorder: order} wait for trade data, then push order data
        self.order_cache: dict[str: OrderData] = {}

    def OnFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def OnFrontDisconnected(self, nReason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{nReason}")

    def OnRspAuthenticate(self, pRspAuthenticate, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """用户授权验证回报"""
        if not pRspInfo.ErrorID:
            self.auth_status = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            # 如果是授权码错误，则禁止再次发起认证
            if pRspInfo.ErrorID == 63:
                self.auth_failed = True
            self.gateway.write_error("交易服务器授权验证失败", pRspInfo)

    def OnRspUserLogin(self, pRspUserLogin: RspUserLoginField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """用户登录请求回报"""
        if not pRspInfo.ErrorID:
            self.frontid = pRspUserLogin.FrontID
            self.sessionid = pRspUserLogin.SessionID
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            # 自动确认结算单
            self.reqid += 1
            pSettlementInfoConfirm = SettlementInfoConfirmField(
                BrokerID=self.brokerid, InvestorID=self.userid, )
            self.ReqSettlementInfoConfirm(pSettlementInfoConfirm, self.reqid)
        else:
            self.login_failed = True
            self.gateway.write_error("交易服务器登录失败", pRspInfo)

    def OnRspOrderInsert(self, pInputOrder: InputOrderField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """委托下单失败回报"""
        order_ref: str = pInputOrder.OrderRef
        orderid: str = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol: str = pInputOrder.InstrumentID
        contract: ContractData = symbol_contract_map[symbol]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            direction=DIRECTION_CTP2VT[pInputOrder.Direction],
            offset=OFFSET_CTP2VT.get(
                pInputOrder.CombOffsetFlag, Offset.NONE),
            price=pInputOrder.LimitPrice,
            volume=pInputOrder.VolumeTotalOriginal,
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)
        self.gateway.write_error("交易委托失败", pRspInfo)

    def OnRspOrderAction(self, pInputOrderAction: InputOrderActionField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """委托撤单失败回报"""
        self.gateway.write_error("交易撤单失败", pRspInfo)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm: InputOrderActionField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """确认结算单回报"""
        self.gateway.write_log("结算信息确认成功")

        # 由于流控，单次查询可能失败，通过while循环持续尝试，直到成功发出请求
        while True:
            self.reqid += 1
            pQryInstrument = QryInstrumentField()
            n: int = self.ReqQryInstrument(pQryInstrument, self.reqid)

            if not n:
                break
            else:
                sleep(1)

    def OnRspQryInvestorPosition(self, pInvestorPosition: InvestorPositionField, pRspInfo: RspInfoField, nRequestID, bIsLast: bool) -> None:
        """持仓查询回报"""
        if not pInvestorPosition:
            return

        # 必须已经收到了合约信息后才能处理
        symbol: str = pInvestorPosition.InstrumentID
        contract: ContractData | None = symbol_contract_map.get(symbol, None)
        direction = pInvestorPosition.PosiDirection

        if contract:
            # 获取之前缓存的持仓数据缓存
            key: str = f"{symbol}.{direction}"
            position: PositionData | None = self.positions.get(key, None)
            if not position:
                position = PositionData(
                    symbol=symbol,
                    exchange=contract.exchange,
                    direction=DIRECTION_CTP2VT[direction],
                    gateway_name=self.gateway_name
                )
                self.positions[key] = position

            # 对于上期所昨仓需要特殊处理
            if position.exchange in {Exchange.SHFE, Exchange.INE}:
                if pInvestorPosition.YdPosition and not pInvestorPosition.TodayPosition:
                    position.yd_volume = pInvestorPosition.Position
            # 对于其他交易所昨仓的计算
            else:
                position.yd_volume = pInvestorPosition.Position - pInvestorPosition.TodayPosition

            # 获取合约的乘数信息
            size: float = contract.size

            # 计算之前已有仓位的持仓总成本
            cost: float = position.price * position.volume * size

            # 累加更新持仓数量和盈亏
            position.volume += pInvestorPosition.Position
            position.pnl += pInvestorPosition.PositionProfit

            # 计算更新后的持仓总成本和均价
            if position.volume and size:
                cost += pInvestorPosition.PositionCost
                position.price = cost / (position.volume * size)

            # 更新仓位冻结数量
            if position.direction == Direction.LONG:
                position.frozen += pInvestorPosition.ShortFrozen
            else:
                position.frozen += pInvestorPosition.LongFrozen

        if bIsLast:
            for position in self.positions.values():
                self.gateway.on_position(position)

            self.positions.clear()

    def OnRspQryTradingAccount(self, pTradingAccount: TradingAccountField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """资金查询回报"""
        if not pTradingAccount.AccountID:
            return

        account: AccountData = AccountData(
            accountid=pTradingAccount.AccountID,
            balance=pTradingAccount.Balance,
            frozen=pTradingAccount.FrozenMargin +
            pTradingAccount.FrozenCash + pTradingAccount.FrozenCommission,
            gateway_name=self.gateway_name
        )
        account.available = pTradingAccount.Available

        self.gateway.on_account(account)

    def OnRspQryInstrument(self, pInstrument: InstrumentField, pRspInfo: RspInfoField, nRequestID, bIsLast) -> None:
        """合约查询回报"""
        product: Product | None = PRODUCT_CTP2VT.get(pInstrument.ProductClass, None)
        if product:
            contract: ContractData = ContractData(
                symbol=pInstrument.InstrumentID,
                exchange=EXCHANGE_CTP2VT[pInstrument.ExchangeID],
                name=pInstrument.InstrumentName,
                product=product,
                size=pInstrument.VolumeMultiple,
                pricetick=pInstrument.PriceTick,
                gateway_name=self.gateway_name
            )

            # 期权相关
            if contract.product == Product.OPTION:
                # 移除郑商所期权产品名称带有的C/P后缀
                if contract.exchange == Exchange.CZCE:
                    contract.option_portfolio = pInstrument.ProductID[:-1]
                else:
                    contract.option_portfolio = pInstrument.ProductID

                contract.option_underlying = pInstrument.UnderlyingInstrID
                contract.option_type = OPTIONTYPE_CTP2VT.get(
                    pInstrument.OptionsType, None)  # type: ignore
                contract.option_strike = pInstrument.StrikePrice
                contract.option_index = str(pInstrument.StrikePrice)
                contract.option_listed = datetime.strptime(
                    pInstrument.OpenDate, "%Y%m%d")
                contract.option_expiry = datetime.strptime(
                    pInstrument.ExpireDate, "%Y%m%d")

            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        if bIsLast:
            self.contract_inited = True
            self.gateway.write_log("合约信息查询成功")

            for pOrder in self.order_data:
                self.OnRtnOrder(pOrder)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()

    def OnRtnOrder(self, pOrder: OrderField) -> None:
        """委托更新推送"""
        if not self.contract_inited:
            self.order_data.append(pOrder)
            return

        # print(pOrder)
        # breakpoint()
        symbol: str = pOrder.InstrumentID
        contract: ContractData = symbol_contract_map[symbol]

        frontid: int = pOrder.FrontID
        sessionid: int = pOrder.SessionID
        order_ref: str = pOrder.OrderRef
        orderid: str = f"{frontid}_{sessionid}_{order_ref}"

        timestamp: str = f"{pOrder.InsertDate} {pOrder.InsertTime}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

        tp: tuple = (pOrder.OrderPriceType, pOrder.TimeCondition, pOrder.VolumeCondition)
        order_type: OrderType | None = ORDERTYPE_CTP2VT.get(tp, None)
        if not order_type:
            self.gateway.write_log(f"收到不支持的委托类型，委托号：{orderid}")
            return

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            type=order_type,
            direction=DIRECTION_CTP2VT[pOrder.Direction],
            offset=OFFSET_CTP2VT[pOrder.CombOffsetFlag],
            price=pOrder.LimitPrice,
            volume=pOrder.VolumeTotalOriginal,
            traded=pOrder.VolumeTraded,
            status=STATUS_CTP2VT[pOrder.OrderStatus],
            datetime=dt,
            gateway_name=self.gateway_name
        )

        old_order = self.active_orders.get(order.orderid)
        if order.is_active():
            self.active_orders[order.orderid] = order
        else:
            if order.orderid in self.active_orders:
                self.active_orders.pop(order.orderid)

        # update order after update trade, if trade exists.
        if old_order:
            # filter duplicated order, traded and status both not changed
            if old_order.traded == order.traded and old_order.status == order.status:
                return
            elif order.traded > old_order.traded:
                self.order_cache[pOrder.OrderSysID] = order
        else:
            self.gateway.on_order(order)

        order_sysid = pOrder.OrderSysID
        if order_sysid not in self.sysid_orderid_map:
            self.sysid_orderid_map[order_sysid] = orderid

    def OnRtnTrade(self, pTrade: TradeField) -> None:
        """成交数据推送"""
        if not self.contract_inited:
            self.trade_data.append(pTrade)
            return

        symbol: str = pTrade.InstrumentID
        contract: ContractData = symbol_contract_map[symbol]

        order_sysid = pTrade.OrderSysID
        orderid: str = self.sysid_orderid_map[order_sysid]

        timestamp: str = f"{pTrade.TradeDate} {pTrade.TradeTime}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            tradeid=pTrade.TradeID,
            direction=DIRECTION_CTP2VT[pTrade.Direction],
            offset=OFFSET_CTP2VT[pTrade.OffsetFlag],
            price=pTrade.Price,
            volume=pTrade.Volume,
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

        # push order after trade; clear order_cache, sysid_orderid_map;
        if order_sysid in self.order_cache:
            order = self.order_cache.pop(order_sysid)
            self.gateway.on_order(order)
            if not order.is_active():
                self.sysid_orderid_map.pop(order_sysid)

    def connect(
        self,
        address: str,
        userid: str,
        password: str,
        brokerid: str,
        auth_code: str,
        appid: str
    ) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.auth_code = auth_code
        self.appid = appid

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.Create((str(path) + "\\Td"))

            self.SubscribePrivateTopic(0)
            self.SubscribePublicTopic(0)

            self.RegisterFront(address)
            self.Init()

            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self) -> None:
        """发起授权验证"""
        if self.auth_failed:
            return

        pReqAuthenticate = ReqAuthenticateField(
            BrokerID=self.brokerid,
            UserID=self.userid,
            AuthCode=self.auth_code,
            AppID=self.appid,
        )

        self.reqid += 1
        self.ReqAuthenticate(pReqAuthenticate, self.reqid)

    def login(self) -> None:
        """用户登录"""
        if self.login_failed:
            return

        pReqUserLogin = ReqUserLoginField(
            UserID=self.userid,
            Password=self.password,
            BrokerID=self.brokerid,
        )

        self.reqid += 1
        self.ReqUserLogin(pReqUserLogin, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2CTP:
            self.gateway.write_log("请选择开平方向")
            return ""

        if req.type not in ORDERTYPE_VT2CTP:
            self.gateway.write_log(f"当前接口不支持该类型的委托{req.type.value}")
            return ""

        self.order_ref += 1

        tp: tuple = ORDERTYPE_VT2CTP[req.type]
        price_type, time_condition, volume_condition = tp

        pInputOrder = InputOrderField(
            InstrumentID=req.symbol,
            ExchangeID=req.exchange.value,
            LimitPrice=req.price,
            VolumeTotalOriginal=int(req.volume),
            OrderPriceType=price_type,
            Direction=DIRECTION_VT2CTP.get(req.direction, ""),
            CombOffsetFlag=OFFSET_VT2CTP.get(req.offset, ""),
            OrderRef=str(self.order_ref),
            InvestorID=self.userid,
            UserID=self.userid,
            BrokerID=self.brokerid,
            CombHedgeFlag=THOST_FTDC_HF_Speculation,
            ContingentCondition=THOST_FTDC_CC_Immediately,
            ForceCloseReason=THOST_FTDC_FCC_NotForceClose,
            IsAutoSuspend=0,
            TimeCondition=time_condition,
            VolumeCondition=volume_condition,
            MinVolume=1
        )

        self.reqid += 1
        n: int = self.ReqOrderInsert(pInputOrder, self.reqid)
        if n:
            self.gateway.write_log(f"委托请求发送失败，错误代码：{n}")
            return ""

        orderid: str = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        frontid, sessionid, order_ref = req.orderid.split("_")

        pInputOrderAction = InputOrderActionField(
            InstrumentID=req.symbol,
            ExchangeID=req.exchange.value,
            OrderRef=order_ref,
            FrontID=int(frontid),
            SessionID=int(sessionid),
            ActionFlag=THOST_FTDC_AF_Delete,
            BrokerID=self.brokerid,
            InvestorID=self.userid
        )

        self.reqid += 1
        self.ReqOrderAction(pInputOrderAction, self.reqid)

    def query_account(self) -> None:
        """查询资金"""
        self.reqid += 1
        pQryTradingAccount = QryTradingAccountField(
            BizType=THOST_FTDC_BZTP_Future)
        self.ReqQryTradingAccount(pQryTradingAccount, self.reqid)

    def query_position(self) -> None:
        """查询持仓"""
        if not symbol_contract_map:
            return

        pQryInvestorPosition = QryInvestorPositionField(
            BrokerID=self.brokerid,
            InvestorID=self.userid
        )

        self.reqid += 1
        self.ReqQryInvestorPosition(pQryInvestorPosition, self.reqid)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.gateway.write_log('CtpTdApi close. ')


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price


def to_str(b: bytes) -> str:
    return b.decode('utf8')
