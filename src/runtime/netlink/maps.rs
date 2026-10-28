//! Anonymous constant maps belong to their lookup rule and are garbage-collected
//! when that rule is flushed. Replacing rules and maps in one batch is atomic.
use super::*;
use nftnl::{
    ProtoFamily, Table,
    expr::{Expression, Register},
    nftnl_sys as sys,
    set::{Set, SetKey},
};

pub(super) struct PortKey;
impl SetKey for PortKey {
    const TYPE: u32 = 13; // nftables inet_service
    const LEN: u32 = 2;
    fn data(&self) -> Box<[u8]> {
        unreachable!("map elements are added with data")
    }
}

pub(super) fn port_map<'a>(
    table: &'a Table,
    id: u32,
    data_type: u32,
    data_len: u32,
) -> Set<'a, PortKey> {
    let set = Set::new(c"__relayd%d", id, table, ProtoFamily::Inet);
    // SAFETY: the owned set is live; these attributes extend the safe set wrapper
    // with map support. Type IDs are nftables userspace datatype IDs.
    unsafe {
        sys::nftnl_set_set_u32(
            set.as_ptr().as_ptr(),
            sys::NFTNL_SET_FLAGS as u16,
            (sys::libc::NFT_SET_ANONYMOUS | sys::libc::NFT_SET_CONSTANT | sys::libc::NFT_SET_MAP)
                as u32,
        );
        sys::nftnl_set_set_u32(
            set.as_ptr().as_ptr(),
            sys::NFTNL_SET_DATA_TYPE as u16,
            data_type,
        );
        sys::nftnl_set_set_u32(
            set.as_ptr().as_ptr(),
            sys::NFTNL_SET_DATA_LEN as u16,
            data_len,
        );
    }
    set
}

pub(super) fn add_element(set: &mut Set<'_, PortKey>, port: u16, data: &[u8]) {
    // SAFETY: libnftnl copies both byte slices and takes ownership of the element.
    unsafe {
        let elem = ptr::NonNull::new(sys::nftnl_set_elem_alloc())
            .expect("nftnl_set_elem_alloc returned null");
        sys::nftnl_set_elem_set(
            elem.as_ptr(),
            sys::NFTNL_SET_ELEM_KEY as u16,
            port.to_be_bytes().as_ptr().cast(),
            2,
        );
        sys::nftnl_set_elem_set(
            elem.as_ptr(),
            sys::NFTNL_SET_ELEM_DATA as u16,
            data.as_ptr().cast(),
            data.len() as u32,
        );
        sys::nftnl_set_elem_add(set.as_ptr().as_ptr(), elem.as_ptr());
    }
}

pub(super) struct MapLookup<'a, 'b> {
    pub set: &'a Set<'b, PortKey>,
    pub destination: Register,
}
impl Expression for MapLookup<'_, '_> {
    fn to_expr(&self, _rule: &nftnl::Rule<'_>) -> ptr::NonNull<sys::nftnl_expr> {
        // SAFETY: all attributes are copied into the newly allocated expression.
        unsafe {
            let expr = ptr::NonNull::new(sys::nftnl_expr_alloc(c"lookup".as_ptr()))
                .expect("nftnl_expr_alloc returned null");
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_LOOKUP_SREG as u16,
                REG_PORT.to_raw(),
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_LOOKUP_DREG as u16,
                self.destination.to_raw(),
            );
            sys::nftnl_expr_set_str(
                expr.as_ptr(),
                sys::NFTNL_EXPR_LOOKUP_SET as u16,
                self.set.get_name().as_ptr(),
            );
            sys::nftnl_expr_set_u32(
                expr.as_ptr(),
                sys::NFTNL_EXPR_LOOKUP_SET_ID as u16,
                self.set.get_id(),
            );
            expr
        }
    }
}
