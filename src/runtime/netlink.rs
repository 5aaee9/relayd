use crate::model::{Allocation, ErrorKind, Protocol, RuntimeStatus};
use crate::runtime::facade::{ListenerMetricsSnapshot, ObservedState, RuntimeError, RuntimeFacade};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

#[cfg(feature = "netlink")]
use std::ffi::{CStr, CString, NulError, c_void};
#[cfg(feature = "netlink")]
use std::io;
#[cfg(feature = "netlink")]
use std::os::fd::AsRawFd;
#[cfg(feature = "netlink")]
use std::ptr;
#[cfg(feature = "netlink")]
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NftDnatRule {
    pub allocation_id: String,
    pub protocol: Protocol,
    pub in_port: u16,
    pub out_host: IpAddr,
    pub out_port: u16,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum NftBackendError {
    #[error("{0}")]
    Apply(String),
}

pub trait NftBackend: Send + Sync {
    fn replace_ruleset(
        &self,
        table: &str,
        chain: &str,
        rules: &[NftDnatRule],
        timeout_ms: u32,
    ) -> Result<(), NftBackendError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetlinkRuntimeConfig {
    pub table: String,
    pub chain: String,
}

impl NetlinkRuntimeConfig {
    pub fn new(table: impl Into<String>, chain: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            chain: chain.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetlinkRuntime<B: NftBackend = LibNftnlBackend> {
    backend: B,
    config: NetlinkRuntimeConfig,
    state: Arc<Mutex<NetlinkState>>,
}

#[derive(Debug, Default)]
struct NetlinkState {
    allocations: HashMap<String, Allocation>,
    stale_error: Option<String>,
    initialized: bool,
}

impl NetlinkRuntime<LibNftnlBackend> {
    pub fn with_libnftnl(config: NetlinkRuntimeConfig) -> Result<Self, NftBackendError> {
        Ok(Self::new(config, LibNftnlBackend::new()?))
    }
}

impl<B: NftBackend> NetlinkRuntime<B> {
    pub fn new(config: NetlinkRuntimeConfig, backend: B) -> Self {
        Self {
            backend,
            config,
            state: Arc::new(Mutex::new(NetlinkState::default())),
        }
    }

    fn apply(
        &self,
        allocations: &HashMap<String, Allocation>,
        timeout_ms: u32,
    ) -> Result<(), NftBackendError> {
        let rules = rules_for_allocations(allocations);
        self.backend
            .replace_ruleset(&self.config.table, &self.config.chain, &rules, timeout_ms)
    }

    fn mark_fresh(state: &mut NetlinkState) {
        state.stale_error = None;
        state.initialized = true;
    }

    fn mark_stale(state: &mut NetlinkState, error: NftBackendError) -> RuntimeError {
        state.stale_error = Some(error.to_string());
        RuntimeError::RuntimeApplyFailed
    }
}

#[derive(Clone, Debug, Default)]
pub struct LibNftnlBackend;

// API notes for the production backend, checked against `nftnl = 0.9.2` and
// `mnl = 0.3.0`: safe wrappers provide `nftnl::Batch`, `Table`, `Chain`,
// `Rule`, `expr::Meta`, `expr::Payload`, `expr::Counter`, `expr::Nat`,
// `mnl::Socket`, and `mnl::cb_run`. Chain flush is represented as
// `NFT_MSG_DELRULE` carrying table+chain attributes via `nftnl_sys`.
// FIB has no safe wrapper, so the local `FibDaddrType` expression writes
// `NFTNL_EXPR_FIB_DREG`, `NFTNL_EXPR_FIB_RESULT`, and `NFTNL_EXPR_FIB_FLAGS`.
#[cfg(feature = "netlink")]
impl LibNftnlBackend {
    pub fn new() -> Result<Self, NftBackendError> {
        Ok(Self)
    }
}

#[cfg(not(feature = "netlink"))]
impl LibNftnlBackend {
    pub fn new() -> Result<Self, NftBackendError> {
        Err(NftBackendError::Apply(
            "netlink runtime requires building relayd with the netlink Cargo feature".to_owned(),
        ))
    }
}

#[cfg(feature = "netlink")]
impl NftBackend for LibNftnlBackend {
    fn replace_ruleset(
        &self,
        table_name: &str,
        chain_name: &str,
        rules: &[NftDnatRule],
        timeout_ms: u32,
    ) -> Result<(), NftBackendError> {
        use nftnl::{Batch, Chain, ChainType, Hook, MsgType, Policy, ProtoFamily, Rule, Table};

        let table_name = cstring_name("table", table_name)?;
        let chain_name = cstring_name("chain", chain_name)?;
        let table = Table::new(table_name.as_c_str(), ProtoFamily::Inet);
        let mut chain = Chain::new(chain_name.as_c_str(), &table);
        chain.set_type(ChainType::Nat);
        chain.set_hook(Hook::PreRouting, NF_IP_PRI_NAT_DST);
        chain.set_policy(Policy::Accept);

        let mut batch = Batch::new();
        batch.add(&EnsureTable::new(table_name.as_c_str()), MsgType::Add);
        batch.add(&chain, MsgType::Add);
        batch.add(
            &FlushChain::new(table_name.as_c_str(), chain_name.as_c_str()),
            MsgType::Del,
        );
        for rule in rules {
            let mut nft_rule = Rule::new(&chain);
            add_dnat_rule_exprs(&mut nft_rule, rule);
            batch.add(&nft_rule, MsgType::Add);
        }

        send_and_process(&batch.finalize(), timeout_ms).map_err(|error| {
            NftBackendError::Apply(format!("nftnl ruleset replace failed: {error}"))
        })
    }
}

#[cfg(not(feature = "netlink"))]
impl NftBackend for LibNftnlBackend {
    fn replace_ruleset(
        &self,
        _table: &str,
        _chain: &str,
        _rules: &[NftDnatRule],
        _timeout_ms: u32,
    ) -> Result<(), NftBackendError> {
        Err(NftBackendError::Apply(
            "netlink runtime requires building relayd with the netlink Cargo feature".to_owned(),
        ))
    }
}

#[cfg(feature = "netlink")]
const NF_IP_PRI_NAT_DST: i32 = -100;
#[cfg(feature = "netlink")]
const NFT_FIB_RESULT_ADDRTYPE: u32 = 1;
#[cfg(feature = "netlink")]
const NFT_FIB_F_DADDR: u32 = 1;
#[cfg(feature = "netlink")]
const RTN_LOCAL: u8 = 2;
#[cfg(feature = "netlink")]
const REG_PROTOCOL: nftnl::expr::Register = nftnl::expr::Register::Reg1;
#[cfg(feature = "netlink")]
const REG_PORT: nftnl::expr::Register = nftnl::expr::Register::Reg2;
#[cfg(feature = "netlink")]
const REG_FIB_TYPE: nftnl::expr::Register = nftnl::expr::Register::Reg3;
#[cfg(feature = "netlink")]
const REG_DNAT_ADDR: nftnl::expr::Register = nftnl::expr::Register::Reg1;
#[cfg(feature = "netlink")]
const REG_DNAT_PORT: nftnl::expr::Register = nftnl::expr::Register::Reg2;

#[cfg(feature = "netlink")]
fn cstring_name(kind: &'static str, value: &str) -> Result<CString, NftBackendError> {
    CString::new(value).map_err(|error| cstring_error(kind, error))
}

#[cfg(feature = "netlink")]
fn cstring_error(kind: &'static str, error: NulError) -> NftBackendError {
    NftBackendError::Apply(format!("invalid nftables {kind} name: {error}"))
}

#[cfg(feature = "netlink")]
fn add_dnat_rule_exprs(rule: &mut nftnl::Rule<'_>, dnat: &NftDnatRule) {
    use nftnl::expr::{Counter, Meta, Nat, NatType};

    let nfproto = nfproto_for(dnat.out_host);
    let l4proto = l4proto_for(dnat.protocol);

    rule.add_expr(&LoadMeta {
        key: Meta::NfProto,
        register: REG_PROTOCOL,
    });
    rule.add_expr(&CmpRegister::new(REG_PROTOCOL, nfproto));
    rule.add_expr(&LoadMeta {
        key: Meta::L4Proto,
        register: REG_PROTOCOL,
    });
    rule.add_expr(&CmpRegister::new(REG_PROTOCOL, l4proto));
    rule.add_expr(&LoadPayload {
        base: nftnl::nftnl_sys::libc::NFT_PAYLOAD_TRANSPORT_HEADER as u32,
        offset: 2,
        len: 2,
        register: REG_PORT,
    });
    rule.add_expr(&CmpRegister::new(REG_PORT, dnat.in_port.to_be()));
    rule.add_expr(&FibDaddrType {
        register: REG_FIB_TYPE,
    });
    rule.add_expr(&CmpRegister::new(REG_FIB_TYPE, RTN_LOCAL));
    match dnat.out_host {
        IpAddr::V4(addr) => rule.add_expr(&ImmediateRegister::new(REG_DNAT_ADDR, addr.octets())),
        IpAddr::V6(addr) => rule.add_expr(&ImmediateRegister::new(REG_DNAT_ADDR, addr.octets())),
    }
    rule.add_expr(&ImmediateRegister::new(
        REG_DNAT_PORT,
        dnat.out_port.to_be(),
    ));
    rule.add_expr(&Counter);
    rule.add_expr(&Nat {
        nat_type: NatType::DNat,
        family: nfproto_family_for(dnat.out_host),
        ip_register: REG_DNAT_ADDR,
        port_register: Some(REG_DNAT_PORT),
    });
}

#[cfg(feature = "netlink")]
fn nfproto_for(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => nftnl::nftnl_sys::libc::NFPROTO_IPV4 as u8,
        IpAddr::V6(_) => nftnl::nftnl_sys::libc::NFPROTO_IPV6 as u8,
    }
}

#[cfg(feature = "netlink")]
fn nfproto_family_for(addr: IpAddr) -> nftnl::ProtoFamily {
    match addr {
        IpAddr::V4(_) => nftnl::ProtoFamily::Ipv4,
        IpAddr::V6(_) => nftnl::ProtoFamily::Ipv6,
    }
}

#[cfg(feature = "netlink")]
fn l4proto_for(protocol: Protocol) -> u8 {
    match protocol {
        Protocol::Tcp => nftnl::nftnl_sys::libc::IPPROTO_TCP as u8,
        Protocol::Udp => nftnl::nftnl_sys::libc::IPPROTO_UDP as u8,
        Protocol::Both => unreachable!("both is expanded before rule projection"),
    }
}

#[cfg(feature = "netlink")]
fn send_and_process(batch: &nftnl::FinalizedBatch, timeout_ms: u32) -> io::Result<()> {
    let socket = mnl::Socket::new(mnl::Bus::Netfilter)?;
    let deadline = apply_deadline(timeout_ms);
    let portid = socket.portid();
    send_batch_with_deadline(&socket, batch, deadline)?;

    let mut buffer = vec![0; nftnl::nft_nlmsg_maxsize() as usize];
    let mut expected_seqs = batch.sequence_numbers();
    while !expected_seqs.is_empty() {
        set_socket_timeouts(&socket, remaining_timeout(deadline)?)?;
        let messages = socket.recv(&mut buffer).map_err(timeout_io_error)?;
        for message in messages {
            let message = message?;
            let expected_seq = expected_seqs
                .next()
                .ok_or_else(|| io::Error::other("received unexpected nftables ACK message"))?;
            mnl::cb_run(message, expected_seq, portid)?;
        }
    }
    Ok(())
}

#[cfg(feature = "netlink")]
fn send_batch_with_deadline(
    socket: &mnl::Socket,
    batch: &nftnl::FinalizedBatch,
    deadline: Instant,
) -> io::Result<()> {
    for message in batch {
        set_socket_timeouts(socket, remaining_timeout(deadline)?)?;
        if socket.send(message)? < message.len() {
            return Err(io::Error::other("sendto did not send entire message"));
        }
    }
    Ok(())
}

#[cfg(feature = "netlink")]
fn apply_deadline(timeout_ms: u32) -> Instant {
    Instant::now() + timeout_duration(timeout_ms)
}

#[cfg(feature = "netlink")]
fn timeout_duration(timeout_ms: u32) -> Duration {
    Duration::from_millis(u64::from(timeout_ms.max(1)))
}

#[cfg(feature = "netlink")]
fn remaining_timeout(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "nftables netlink apply timed out"))
}

#[cfg(feature = "netlink")]
fn timeout_io_error(error: io::Error) -> io::Error {
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    ) {
        io::Error::new(io::ErrorKind::TimedOut, "nftables netlink apply timed out")
    } else {
        error
    }
}

#[cfg(feature = "netlink")]
fn set_socket_timeouts(socket: &mnl::Socket, duration: Duration) -> io::Result<()> {
    let timeout = duration_to_timeval(duration);
    set_socket_timeout(socket, mnl::mnl_sys::libc::SO_RCVTIMEO, timeout)?;
    set_socket_timeout(socket, mnl::mnl_sys::libc::SO_SNDTIMEO, timeout)
}

#[cfg(feature = "netlink")]
fn duration_to_timeval(duration: Duration) -> mnl::mnl_sys::libc::timeval {
    mnl::mnl_sys::libc::timeval {
        tv_sec: duration.as_secs().try_into().unwrap_or(i64::MAX),
        tv_usec: duration.subsec_micros().into(),
    }
}

#[cfg(feature = "netlink")]
fn set_socket_timeout(
    socket: &mnl::Socket,
    option: mnl::mnl_sys::libc::c_int,
    timeout: mnl::mnl_sys::libc::timeval,
) -> io::Result<()> {
    let result = unsafe {
        mnl::mnl_sys::libc::setsockopt(
            socket.as_raw_fd(),
            mnl::mnl_sys::libc::SOL_SOCKET,
            option,
            &timeout as *const _ as *const c_void,
            std::mem::size_of_val(&timeout)
                .try_into()
                .expect("timeval size fits socklen_t"),
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(feature = "netlink")]
struct EnsureTable<'a> {
    name: &'a CStr,
}

#[cfg(feature = "netlink")]
impl<'a> EnsureTable<'a> {
    fn new(name: &'a CStr) -> Self {
        Self { name }
    }
}

#[cfg(feature = "netlink")]
unsafe impl nftnl::NlMsg for EnsureTable<'_> {
    unsafe fn write(&self, buf: *mut c_void, seq: u32, _msg_type: nftnl::MsgType) {
        use nftnl::nftnl_sys as sys;

        let table = TableHandle::new();
        // SAFETY: `table` is freshly allocated and `self.name` lives until serialization returns.
        unsafe {
            sys::nftnl_table_set_u32(
                table.as_ptr(),
                sys::NFTNL_TABLE_FAMILY as u16,
                nftnl::ProtoFamily::Inet as u32,
            );
            sys::nftnl_table_set_str(
                table.as_ptr(),
                sys::NFTNL_TABLE_NAME as u16,
                self.name.as_ptr(),
            );
            sys::nftnl_table_set_u32(table.as_ptr(), sys::NFTNL_TABLE_FLAGS as u16, 0);
            let header = sys::nftnl_nlmsg_build_hdr(
                buf.cast(),
                sys::libc::NFT_MSG_NEWTABLE as u16,
                nftnl::ProtoFamily::Inet as u16,
                (sys::libc::NLM_F_ACK | sys::libc::NLM_F_CREATE) as u16,
                seq,
            );
            sys::nftnl_table_nlmsg_build_payload(header, table.as_ptr());
        }
    }
}

#[cfg(feature = "netlink")]
struct TableHandle(ptr::NonNull<nftnl::nftnl_sys::nftnl_table>);

#[cfg(feature = "netlink")]
impl TableHandle {
    fn new() -> Self {
        let ptr = ptr::NonNull::new(unsafe { nftnl::nftnl_sys::nftnl_table_alloc() })
            .expect("nftnl_table_alloc returned null");
        Self(ptr)
    }

    fn as_ptr(&self) -> *mut nftnl::nftnl_sys::nftnl_table {
        self.0.as_ptr()
    }
}

#[cfg(feature = "netlink")]
impl Drop for TableHandle {
    fn drop(&mut self) {
        unsafe { nftnl::nftnl_sys::nftnl_table_free(self.0.as_ptr()) };
    }
}

#[cfg(feature = "netlink")]
struct FlushChain<'a> {
    table: &'a CStr,
    chain: &'a CStr,
}

#[cfg(feature = "netlink")]
impl<'a> FlushChain<'a> {
    fn new(table: &'a CStr, chain: &'a CStr) -> Self {
        Self { table, chain }
    }
}

#[cfg(feature = "netlink")]
unsafe impl nftnl::NlMsg for FlushChain<'_> {
    unsafe fn write(&self, buf: *mut c_void, seq: u32, _msg_type: nftnl::MsgType) {
        use nftnl::nftnl_sys as sys;

        let rule = RuleHandle::new();
        // SAFETY: `rule` is freshly allocated and these C strings outlive serialization.
        unsafe {
            sys::nftnl_rule_set_u32(
                rule.as_ptr(),
                sys::NFTNL_RULE_FAMILY as u16,
                nftnl::ProtoFamily::Inet as u32,
            );
            sys::nftnl_rule_set_str(
                rule.as_ptr(),
                sys::NFTNL_RULE_TABLE as u16,
                self.table.as_ptr(),
            );
            sys::nftnl_rule_set_str(
                rule.as_ptr(),
                sys::NFTNL_RULE_CHAIN as u16,
                self.chain.as_ptr(),
            );
            let header = sys::nftnl_nlmsg_build_hdr(
                buf.cast(),
                sys::libc::NFT_MSG_DELRULE as u16,
                nftnl::ProtoFamily::Inet as u16,
                sys::libc::NLM_F_ACK as u16,
                seq,
            );
            sys::nftnl_rule_nlmsg_build_payload(header, rule.as_ptr());
        }
    }
}

#[cfg(feature = "netlink")]
struct RuleHandle(ptr::NonNull<nftnl::nftnl_sys::nftnl_rule>);

#[cfg(feature = "netlink")]
impl RuleHandle {
    fn new() -> Self {
        let ptr = ptr::NonNull::new(unsafe { nftnl::nftnl_sys::nftnl_rule_alloc() })
            .expect("nftnl_rule_alloc returned null");
        Self(ptr)
    }

    fn as_ptr(&self) -> *mut nftnl::nftnl_sys::nftnl_rule {
        self.0.as_ptr()
    }
}

#[cfg(feature = "netlink")]
impl Drop for RuleHandle {
    fn drop(&mut self) {
        unsafe { nftnl::nftnl_sys::nftnl_rule_free(self.0.as_ptr()) };
    }
}

#[cfg(feature = "netlink")]
struct LoadMeta {
    key: nftnl::expr::Meta,
    register: nftnl::expr::Register,
}

#[cfg(feature = "netlink")]
impl nftnl::expr::Expression for LoadMeta {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;

        let expr = ptr::NonNull::new(unsafe { sys::nftnl_expr_alloc(c"meta".as_ptr()) })
            .expect("nftnl_expr_alloc returned null");
        // SAFETY: expression is newly allocated and attributes match the meta expression ABI.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_META_DREG as u16,
                self.register.to_raw(),
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_META_KEY as u16,
                self.key.to_raw_key(),
            );
        }
        expr
    }
}

#[cfg(feature = "netlink")]
struct LoadPayload {
    base: u32,
    offset: u32,
    len: u32,
    register: nftnl::expr::Register,
}

#[cfg(feature = "netlink")]
impl nftnl::expr::Expression for LoadPayload {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;

        let expr = ptr::NonNull::new(unsafe { sys::nftnl_expr_alloc(c"payload".as_ptr()) })
            .expect("nftnl_expr_alloc returned null");
        // SAFETY: expression is newly allocated and fields are derived from nftnl's payload API.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_PAYLOAD_BASE as u16,
                self.base,
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_PAYLOAD_OFFSET as u16,
                self.offset,
            );
            sys::nftnl_expr_set_u32(expr.as_ptr(), sys::NFTNL_EXPR_PAYLOAD_LEN as u16, self.len);
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_PAYLOAD_DREG as u16,
                self.register.to_raw(),
            );
        }
        expr
    }
}

#[cfg(feature = "netlink")]
struct CmpRegister<T> {
    register: nftnl::expr::Register,
    data: T,
}

#[cfg(feature = "netlink")]
impl<T> CmpRegister<T> {
    fn new(register: nftnl::expr::Register, data: T) -> Self {
        Self { register, data }
    }
}

#[cfg(feature = "netlink")]
impl<T: Copy> nftnl::expr::Expression for CmpRegister<T> {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;

        let expr = ptr::NonNull::new(unsafe { sys::nftnl_expr_alloc(c"cmp".as_ptr()) })
            .expect("nftnl_expr_alloc returned null");
        // SAFETY: expression is newly allocated; data points to `self.data` for this call only
        // and libnftnl copies it into the expression.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_CMP_SREG as u16,
                self.register.to_raw(),
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_CMP_OP as u16,
                sys::libc::NFT_CMP_EQ as u32,
            );
            sys::nftnl_expr_set(
                expr.as_ptr(),
                sys::NFTNL_EXPR_CMP_DATA as u16,
                &self.data as *const _ as *const c_void,
                size_of_data::<T>(),
            );
        }
        expr
    }
}

#[cfg(feature = "netlink")]
struct ImmediateRegister<T> {
    register: nftnl::expr::Register,
    data: T,
}

#[cfg(feature = "netlink")]
impl<T> ImmediateRegister<T> {
    fn new(register: nftnl::expr::Register, data: T) -> Self {
        Self { register, data }
    }
}

#[cfg(feature = "netlink")]
impl<T: Copy> nftnl::expr::Expression for ImmediateRegister<T> {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;

        let expr = ptr::NonNull::new(unsafe { sys::nftnl_expr_alloc(c"immediate".as_ptr()) })
            .expect("nftnl_expr_alloc returned null");
        // SAFETY: expression is newly allocated; libnftnl copies the immediate bytes.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_IMM_DREG as u16,
                self.register.to_raw(),
            );
            sys::nftnl_expr_set(
                expr.as_ptr(),
                sys::NFTNL_EXPR_IMM_DATA as u16,
                &self.data as *const _ as *const c_void,
                size_of_data::<T>(),
            );
        }
        expr
    }
}

#[cfg(feature = "netlink")]
struct FibDaddrType {
    register: nftnl::expr::Register,
}

#[cfg(feature = "netlink")]
impl nftnl::expr::Expression for FibDaddrType {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<nftnl::nftnl_sys::nftnl_expr> {
        use nftnl::nftnl_sys as sys;

        let expr = ptr::NonNull::new(unsafe { sys::nftnl_expr_alloc(c"fib".as_ptr()) })
            .expect("nftnl_expr_alloc returned null");
        // SAFETY: expression is newly allocated and FIB attributes match libnftnl's ABI.
        unsafe {
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_FIB_DREG as u16,
                self.register.to_raw(),
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_FIB_RESULT as u16,
                NFT_FIB_RESULT_ADDRTYPE,
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_FIB_FLAGS as u16,
                NFT_FIB_F_DADDR,
            );
        }
        expr
    }
}

#[cfg(feature = "netlink")]
fn size_of_data<T>() -> u32 {
    std::mem::size_of::<T>()
        .try_into()
        .expect("nft expression immediate data size fits in u32")
}

#[async_trait]
impl<B: NftBackend> RuntimeFacade for NetlinkRuntime<B> {
    async fn initialize(&self, timeout_ms: u32) -> Result<(), RuntimeError> {
        let snapshot = self.state.lock().unwrap().allocations.clone();
        match self.apply(&snapshot, timeout_ms) {
            Ok(()) => {
                Self::mark_fresh(&mut self.state.lock().unwrap());
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                Err(Self::mark_stale(&mut state, error))
            }
        }
    }

    async fn create(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut candidate = self.state.lock().unwrap().allocations.clone();
        candidate.insert(allocation.id.clone(), allocation.clone());
        match self.apply(&candidate, timeout_ms) {
            Ok(()) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Self::mark_fresh(&mut state);
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                Err(Self::mark_stale(&mut state, error))
            }
        }
    }

    async fn update(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut candidate = self.state.lock().unwrap().allocations.clone();
        candidate.insert(allocation.id.clone(), allocation.clone());
        match self.apply(&candidate, timeout_ms) {
            Ok(()) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Self::mark_fresh(&mut state);
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Err(Self::mark_stale(&mut state, error))
            }
        }
    }

    async fn delete(&self, id: &str, timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut candidate = self.state.lock().unwrap().allocations.clone();
        candidate.remove(id);
        match self.apply(&candidate, timeout_ms) {
            Ok(()) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Self::mark_fresh(&mut state);
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                Err(Self::mark_stale(&mut state, error))
            }
        }
    }

    async fn restore(&self, allocation: &Allocation, timeout_ms: u32) -> Result<(), RuntimeError> {
        let mut candidate = self.state.lock().unwrap().allocations.clone();
        candidate.insert(allocation.id.clone(), allocation.clone());
        match self.apply(&candidate, timeout_ms) {
            Ok(()) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Self::mark_fresh(&mut state);
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                state.allocations = candidate;
                Err(Self::mark_stale(&mut state, error))
            }
        }
    }

    async fn snapshot(&self, id: &str) -> Result<Option<ObservedState>, RuntimeError> {
        let state = self.state.lock().unwrap();
        Ok(state.allocations.get(id).map(|allocation| {
            if bound_target(allocation).is_none() {
                return observed_state_for(allocation);
            }

            if let Some(error) = &state.stale_error {
                return ObservedState {
                    effective_target_port: allocation.target_port,
                    effective_host: allocation.host.clone(),
                    runtime_status: RuntimeStatus::DegradedApplyFailed,
                    error_kind: Some(ErrorKind::ApplyFailed),
                    last_error: Some(error.clone()),
                };
            }

            observed_state_for(allocation)
        }))
    }

    async fn snapshot_listener_metrics(
        &self,
    ) -> Result<Vec<ListenerMetricsSnapshot>, RuntimeError> {
        Ok(Vec::new())
    }
}

fn observed_state_for(allocation: &Allocation) -> ObservedState {
    match bound_target(allocation) {
        Some((host, port)) => ObservedState {
            effective_target_port: Some(port),
            effective_host: Some(host.to_string()),
            runtime_status: RuntimeStatus::Active,
            error_kind: None,
            last_error: None,
        },
        None => ObservedState {
            effective_target_port: allocation.target_port,
            effective_host: allocation.host.clone(),
            runtime_status: RuntimeStatus::RejectingNoHost,
            error_kind: None,
            last_error: None,
        },
    }
}

fn rules_for_allocations(allocations: &HashMap<String, Allocation>) -> Vec<NftDnatRule> {
    let mut allocations: Vec<&Allocation> = allocations.values().collect();
    allocations.sort_by(|left, right| left.id.cmp(&right.id));
    allocations
        .into_iter()
        .flat_map(|allocation| {
            let Some((out_host, out_port)) = bound_target(allocation) else {
                return Vec::new();
            };
            protocols_for(allocation.protocol)
                .into_iter()
                .map(|protocol| NftDnatRule {
                    allocation_id: allocation.id.clone(),
                    protocol,
                    in_port: allocation.port,
                    out_host,
                    out_port,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn bound_target(allocation: &Allocation) -> Option<(IpAddr, u16)> {
    Some((
        allocation.host.as_ref()?.parse().ok()?,
        allocation.target_port?,
    ))
}

fn protocols_for(protocol: Protocol) -> Vec<Protocol> {
    match protocol {
        Protocol::Tcp => vec![Protocol::Tcp],
        Protocol::Udp => vec![Protocol::Udp],
        Protocol::Both => vec![Protocol::Tcp, Protocol::Udp],
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[derive(Clone, Debug, Default)]
    pub(crate) struct RecordingBackend {
        state: Arc<Mutex<RecordingState>>,
    }

    #[derive(Debug, Default)]
    struct RecordingState {
        calls: Vec<RecordingCall>,
        fail_next: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct RecordingCall {
        pub table: String,
        pub chain: String,
        pub rules: Vec<NftDnatRule>,
        pub timeout_ms: u32,
    }

    impl RecordingBackend {
        pub(crate) fn fail_next(&self, error: impl Into<String>) {
            self.state.lock().unwrap().fail_next = Some(error.into());
        }

        pub(crate) fn calls(&self) -> Vec<RecordingCall> {
            self.state.lock().unwrap().calls.clone()
        }
    }

    impl NftBackend for RecordingBackend {
        fn replace_ruleset(
            &self,
            table: &str,
            chain: &str,
            rules: &[NftDnatRule],
            timeout_ms: u32,
        ) -> Result<(), NftBackendError> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(RecordingCall {
                table: table.to_owned(),
                chain: chain.to_owned(),
                rules: rules.to_vec(),
                timeout_ms,
            });
            if let Some(error) = state.fail_next.take() {
                return Err(NftBackendError::Apply(error));
            }
            Ok(())
        }
    }

    fn runtime() -> (NetlinkRuntime<RecordingBackend>, RecordingBackend) {
        let backend = RecordingBackend::default();
        (
            NetlinkRuntime::new(
                NetlinkRuntimeConfig::new("relayd", "mapping"),
                backend.clone(),
            ),
            backend,
        )
    }

    fn allocation(
        id: &str,
        protocol: Protocol,
        port: u16,
        target_port: Option<u16>,
        host: Option<&str>,
    ) -> Allocation {
        Allocation {
            id: id.to_owned(),
            protocol,
            port,
            target_port,
            host: host.map(str::to_owned),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        }
    }

    fn rule(
        allocation_id: &str,
        protocol: Protocol,
        in_port: u16,
        out_host: &str,
        out_port: u16,
    ) -> NftDnatRule {
        NftDnatRule {
            allocation_id: allocation_id.to_owned(),
            protocol,
            in_port,
            out_host: out_host.parse().unwrap(),
            out_port,
        }
    }

    #[tokio::test]
    async fn initialize_replaces_with_empty_owned_ruleset() {
        let (runtime, backend) = runtime();

        runtime.initialize(500).await.unwrap();

        assert_eq!(
            backend.calls(),
            vec![RecordingCall {
                table: "relayd".to_owned(),
                chain: "mapping".to_owned(),
                rules: Vec::new(),
                timeout_ms: 500,
            }]
        );
    }

    #[tokio::test]
    async fn unbound_allocation_records_state_without_rules() {
        let (runtime, backend) = runtime();
        let allocation = allocation("a1", Protocol::Tcp, 10000, None, None);

        runtime.create(&allocation, 500).await.unwrap();

        assert!(backend.calls().last().unwrap().rules.is_empty());
        let observed = runtime.snapshot("a1").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(observed.effective_target_port, None);
        assert_eq!(observed.effective_host, None);
    }

    #[tokio::test]
    async fn bound_tcp_v4_and_udp_v6_create_rules() {
        let (runtime, backend) = runtime();

        runtime
            .create(
                &allocation("tcp", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();
        runtime
            .create(
                &allocation("udp", Protocol::Udp, 10001, Some(5353), Some("2001:db8::1")),
                500,
            )
            .await
            .unwrap();

        assert_eq!(
            backend.calls().last().unwrap().rules,
            vec![
                rule("tcp", Protocol::Tcp, 10000, "127.0.0.1", 8080),
                rule("udp", Protocol::Udp, 10001, "2001:db8::1", 5353),
            ]
        );
    }

    #[tokio::test]
    async fn both_protocol_expands_to_tcp_and_udp_rules() {
        let (runtime, backend) = runtime();

        runtime
            .create(
                &allocation("both", Protocol::Both, 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();

        assert_eq!(
            backend.calls().last().unwrap().rules,
            vec![
                rule("both", Protocol::Tcp, 10000, "127.0.0.1", 8080),
                rule("both", Protocol::Udp, 10000, "127.0.0.1", 8080),
            ]
        );
    }

    #[tokio::test]
    async fn update_and_delete_rewrite_complete_ruleset() {
        let (runtime, backend) = runtime();
        runtime
            .create(&allocation("a1", Protocol::Tcp, 10000, None, None), 500)
            .await
            .unwrap();

        runtime
            .update(
                &allocation("a1", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();
        assert_eq!(
            backend.calls().last().unwrap().rules,
            vec![rule("a1", Protocol::Tcp, 10000, "127.0.0.1", 8080)]
        );

        runtime
            .update(&allocation("a1", Protocol::Tcp, 10000, None, None), 500)
            .await
            .unwrap();
        assert!(backend.calls().last().unwrap().rules.is_empty());

        runtime.delete("a1", 500).await.unwrap();
        assert!(backend.calls().last().unwrap().rules.is_empty());
        assert_eq!(runtime.snapshot("a1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn create_apply_failure_does_not_commit_new_allocation_and_marks_existing_stale() {
        let (runtime, backend) = runtime();
        runtime
            .create(
                &allocation(
                    "existing",
                    Protocol::Tcp,
                    10000,
                    Some(8080),
                    Some("127.0.0.1"),
                ),
                500,
            )
            .await
            .unwrap();
        backend.fail_next("boom");

        assert_eq!(
            runtime
                .create(
                    &allocation("a1", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                    500,
                )
                .await,
            Err(RuntimeError::RuntimeApplyFailed)
        );

        assert_eq!(runtime.snapshot("a1").await.unwrap(), None);
        let existing = runtime.snapshot("existing").await.unwrap().unwrap();
        assert_eq!(existing.runtime_status, RuntimeStatus::DegradedApplyFailed);
        assert_eq!(existing.error_kind, Some(ErrorKind::ApplyFailed));
        assert_eq!(existing.last_error, Some("boom".to_owned()));
    }

    #[tokio::test]
    async fn stale_runtime_keeps_unbound_allocations_rejecting_no_host() {
        let (runtime, backend) = runtime();
        runtime
            .create(
                &allocation("unbound", Protocol::Tcp, 10000, None, None),
                500,
            )
            .await
            .unwrap();

        backend.fail_next("boom");
        assert_eq!(
            runtime
                .create(
                    &allocation("bound", Protocol::Tcp, 10001, Some(8080), Some("127.0.0.1")),
                    500,
                )
                .await,
            Err(RuntimeError::RuntimeApplyFailed)
        );

        let observed = runtime.snapshot("unbound").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::RejectingNoHost);
        assert_eq!(observed.error_kind, None);
        assert_eq!(observed.last_error, None);
    }

    #[tokio::test]
    async fn update_and_restore_failures_commit_stale_state_with_backend_error() {
        let (runtime, backend) = runtime();
        runtime
            .create(&allocation("a1", Protocol::Tcp, 10000, None, None), 500)
            .await
            .unwrap();

        backend.fail_next("boom");
        assert_eq!(
            runtime
                .update(
                    &allocation("a1", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                    500,
                )
                .await,
            Err(RuntimeError::RuntimeApplyFailed)
        );
        let observed = runtime.snapshot("a1").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::DegradedApplyFailed);
        assert_eq!(observed.error_kind, Some(ErrorKind::ApplyFailed));
        assert_eq!(observed.last_error, Some("boom".to_owned()));

        backend.fail_next("boom");
        assert_eq!(
            runtime
                .restore(
                    &allocation("a2", Protocol::Udp, 10001, Some(5353), Some("2001:db8::1")),
                    500,
                )
                .await,
            Err(RuntimeError::RuntimeApplyFailed)
        );
        assert_eq!(
            runtime
                .snapshot("a2")
                .await
                .unwrap()
                .unwrap()
                .runtime_status,
            RuntimeStatus::DegradedApplyFailed
        );
    }

    #[tokio::test]
    async fn delete_apply_failure_keeps_allocation_stale() {
        let (runtime, backend) = runtime();
        runtime
            .create(
                &allocation("a1", Protocol::Tcp, 10000, Some(8080), Some("127.0.0.1")),
                500,
            )
            .await
            .unwrap();

        backend.fail_next("boom");
        assert_eq!(
            runtime.delete("a1", 500).await,
            Err(RuntimeError::RuntimeApplyFailed)
        );

        let observed = runtime.snapshot("a1").await.unwrap().unwrap();
        assert_eq!(observed.runtime_status, RuntimeStatus::DegradedApplyFailed);
        assert_eq!(observed.effective_target_port, Some(8080));
    }

    #[tokio::test]
    async fn listener_metrics_are_empty() {
        let (runtime, _backend) = runtime();

        assert!(
            runtime
                .snapshot_listener_metrics()
                .await
                .unwrap()
                .is_empty()
        );
    }
}
