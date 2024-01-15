use std::{
    ffi::{c_char, c_int, c_void, CStr, CString},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use log::warn;

use crate::db::Db;

use client::{DirEntry, FuseClient};

use self::client::Filetype;

mod client;
mod sys;

const FUSE_CLIENT_OPERATIONS: sys::fuse_operations = generate_fuse_ops();

macro_rules! c_call_errno_neg_1 {
    ($fn:ident $(, $args:expr)*) => {
        {
            let ret = $fn($( $args ),*);
            if ret == -1 {
                return -std::io::Error::last_os_error().raw_os_error().expect("errno value should map to os");
            }
            ret
        }
    };
}

macro_rules! resolve_passthrough_path {
    ($client:ident, $path:ident) => {{
        match $client.get_passthrough_path(c_to_rust_path($path)) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return -1;
            }
            Err(e) => return -e.raw_os_error().expect("Io error should have valid code"),
        }
    }};
}

macro_rules! log_error_chain {
    ($s:expr, $err:expr) => {{
        use std::error::Error;
        let original_err = $err;
        let mut err_log = original_err.to_string();
        let mut cause_log = String::new();
        let mut err: &dyn Error = &original_err;
        while let Some(source) = err.source() {
            cause_log.push_str(&err.to_string());
            err = source
        }

        if !cause_log.is_empty() {
            err_log.push_str("\nCaused by:\n");
        }
        ::log::error!("{}: {}{}", $s, err_log, cause_log);
    }};
}

unsafe fn c_to_rust_path(s: *const c_char) -> &'static Path {
    Path::new(
        CStr::from_ptr(s)
            .to_str()
            .expect("file paths should be valid rust strings"),
    )
}

unsafe fn rust_to_c_path(s: PathBuf) -> CString {
    CString::new(s.to_owned().into_os_string().into_encoded_bytes())
        .expect("rust paths should be valid c strings")
}

unsafe fn get_client() -> &'static mut FuseClient {
    let context = sys::fuse_get_context();
    let client = (*context).private_data as *mut FuseClient;
    &mut *client
}

unsafe extern "C" fn fuse_client_getattr(path: *const c_char, statbuf: *mut sys::stat) -> c_int {
    let client = get_client();
    let rust_path = c_to_rust_path(path);

    let passthrough_path = match client.get_passthrough_path(rust_path) {
        Ok(v) => v,
        Err(e) => return -e.raw_os_error().expect("error value should have code"),
    };

    if let Some(p) = passthrough_path {
        use sys::lstat;
        println!("found passthrough path: {:?}", p);
        return c_call_errno_neg_1!(lstat, rust_to_c_path(p).as_ptr(), statbuf);
    }

    match client.get_filetype(rust_path) {
        Ok(Filetype::Dir) => {
            (*statbuf).st_mode = sys::S_IFDIR | 0o755;
        }
        Ok(Filetype::Link) => {
            (*statbuf).st_mode = sys::S_IFLNK | 0o777;
        }
        Err(e) => {
            log_error_chain!("failed to get attr", e);
            return -1;
        }
    }

    0
}

unsafe extern "C" fn fuse_client_readdir(
    path: *const c_char,
    buf: *mut c_void,
    mut filler: sys::fuse_fill_dir_t,
    _offset: sys::off_t,
    _info: *mut sys::fuse_file_info,
) -> c_int {
    let client = get_client();
    let filler = filler.as_mut().expect("fuse provided invalid dir filler");

    let it = match client.readdir(c_to_rust_path(path)) {
        Ok(v) => v,
        Err(e) => {
            log_error_chain!("failed to readdir", e);
            return -1;
        }
    };
    for item in it {
        // FIXME: fill stat buf
        let name = match item {
            DirEntry::Dir(name) => name,
            DirEntry::File(name) => name,
            DirEntry::Link(name) => name,
        };
        let name =
            CString::new(name.into_encoded_bytes()).expect("rust paths should be valid cstrings");
        filler(buf, name.as_ptr(), std::ptr::null(), 0);
    }

    0
}

unsafe extern "C" fn fuse_client_open(
    path: *const c_char,
    info: *mut sys::fuse_file_info,
) -> c_int {
    let client = get_client();
    let rust_path = c_to_rust_path(path);

    let passthrough_path = match client.get_passthrough_path(rust_path) {
        Ok(v) => v,
        Err(e) => return -e.raw_os_error().expect("error value should have code"),
    };

    if let Some(p) = passthrough_path {
        use sys::open;
        println!("Trying to open: {:?}", p);
        let ret = c_call_errno_neg_1!(open, rust_to_c_path(p).as_ptr(), (*info).flags);
        (*info).fh = ret.try_into().expect("file handle cannot caset to u64");
        return 0;
    }

    warn!("Unimplmeented open for {:?}", rust_path);
    -1
}

unsafe extern "C" fn fuse_client_create(
    path: *const c_char,
    mode: sys::mode_t,
    info: *mut sys::fuse_file_info,
) -> c_int {
    let client = get_client();
    let rust_path = c_to_rust_path(path);

    let passthrough_path = match client.get_passthrough_path(rust_path) {
        Ok(v) => v,
        Err(e) => return -e.raw_os_error().expect("error value should have code"),
    };

    if let Some(p) = passthrough_path {
        use sys::open;
        let ret = c_call_errno_neg_1!(open, rust_to_c_path(p).as_ptr(), (*info).flags, mode);
        (*info).fh = ret.try_into().expect("file handle cannot cast to u64");
    }

    warn!("mapped_path in create {:?}", rust_path);

    -1
}
unsafe extern "C" fn fuse_client_chmod(
    _arg1: *const ::std::os::raw::c_char,
    _arg2: sys::mode_t,
) -> ::std::os::raw::c_int {
    warn!("unimplemented chmod");
    0
}
unsafe extern "C" fn fuse_client_chown(
    _arg1: *const ::std::os::raw::c_char,
    _arg2: sys::uid_t,
    _arg3: sys::gid_t,
) -> ::std::os::raw::c_int {
    warn!("unimplemented chown");
    0
}
unsafe extern "C" fn fuse_client_truncate(
    _arg1: *const ::std::os::raw::c_char,
    _arg2: sys::off_t,
) -> ::std::os::raw::c_int {
    warn!("unimplemented truncate");
    0
}

unsafe extern "C" fn fuse_client_utimens(
    _arg1: *const ::std::os::raw::c_char,
    _tv: *const sys::timespec,
) -> ::std::os::raw::c_int {
    warn!("unimplemented utimens");
    0
}

unsafe extern "C" fn fuse_client_write(
    path: *const ::std::os::raw::c_char,
    buf: *const ::std::os::raw::c_char,
    size: usize,
    offset: sys::off_t,
    info: *mut sys::fuse_file_info,
) -> ::std::os::raw::c_int {
    let client = get_client();
    let rust_path = resolve_passthrough_path!(client, path);

    if (*info).fh == 0 {
        use sys::open;
        let ret = c_call_errno_neg_1!(
            open,
            rust_to_c_path(rust_path).as_ptr(),
            sys::O_WRONLY as i32
        );
        (*info).fh = ret.try_into().expect("file handle cannot cast to u64");
    }

    use sys::pwrite;
    let ret = c_call_errno_neg_1!(
        pwrite,
        (*info)
            .fh
            .try_into()
            .expect("file handle is not a valid i32"),
        buf as *mut c_void,
        size,
        offset
    );

    ret.try_into().expect("write returned invalid return code")
}

unsafe extern "C" fn fuse_client_read(
    path: *const ::std::os::raw::c_char,
    buf: *mut ::std::os::raw::c_char,
    size: usize,
    offset: sys::off_t,
    info: *mut sys::fuse_file_info,
) -> ::std::os::raw::c_int {
    let client = get_client();
    let rust_path = resolve_passthrough_path!(client, path);

    if (*info).fh == 0 {
        use sys::open;
        let ret = c_call_errno_neg_1!(
            open,
            rust_to_c_path(rust_path).as_ptr(),
            sys::O_RDONLY as i32
        );
        (*info).fh = ret.try_into().expect("file handle cannot cast to u64");
    }

    use sys::pread;
    let ret = c_call_errno_neg_1!(
        pread,
        (*info)
            .fh
            .try_into()
            .expect("file handle is not a valid i32"),
        buf as *mut c_void,
        size,
        offset
    );

    ret.try_into().expect("return value not castable to i32")
}

unsafe extern "C" fn fuse_client_readlink(
    path: *const ::std::os::raw::c_char,
    buf: *mut ::std::os::raw::c_char,
    bufsize: usize,
) -> ::std::os::raw::c_int {
    let client = get_client();
    let rust_path = c_to_rust_path(path);
    let passthrough_path = match client.get_passthrough_path(rust_path) {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to retrieve passthrough path: {e}");
            return -1;
        }
    };

    if let Some(passthrough_path) = passthrough_path {
        use sys::readlink;
        println!("resolved as passthrough path: {passthrough_path:?}");
        return c_call_errno_neg_1!(
            readlink,
            rust_to_c_path(passthrough_path).as_ptr(),
            buf,
            bufsize
        ) as i32;
    }

    let link = match client.readlink(rust_path) {
        Ok(v) => v,
        Err(e) => {
            log::error!("failed to read link: {e}");
            return -1;
        }
    };

    println!("Resolved link: {link:?}");
    let link = link.into_os_string().into_encoded_bytes();

    let copy_size = link.len().min(bufsize - 1);
    std::ptr::copy(link.as_ptr(), buf as *mut u8, copy_size);
    *buf.add(copy_size) = 0;

    0
}

unsafe extern "C" fn fuse_client_flush(
    _path: *const c_char,
    _info: *mut sys::fuse_file_info,
) -> ::std::os::raw::c_int {
    // No cache to clear
    0
}

const fn generate_fuse_ops() -> sys::fuse_operations {
    unsafe {
        let mut ops: sys::fuse_operations = MaybeUninit::zeroed().assume_init();
        ops.getattr = Some(fuse_client_getattr);
        ops.readdir = Some(fuse_client_readdir);
        ops.open = Some(fuse_client_open);
        ops.create = Some(fuse_client_create);
        ops.chmod = Some(fuse_client_chmod);
        ops.chown = Some(fuse_client_chown);
        ops.truncate = Some(fuse_client_truncate);
        ops.utimens = Some(fuse_client_utimens);
        ops.write = Some(fuse_client_write);
        ops.read = Some(fuse_client_read);
        ops.flush = Some(fuse_client_flush);
        ops.readlink = Some(fuse_client_readlink);
        ops
    }
}

pub fn run_fuse_client(db: Db, args: impl Iterator<Item = String>) {
    let mut client = FuseClient { db };
    let args: Vec<CString> = args
        .map(|s| CString::new(s).expect("input args not valid c strings"))
        .collect();
    let mut args: Vec<*mut i8> = args.into_iter().map(|s| s.into_raw()).collect();

    let mut args = sys::fuse_args {
        argc: args
            .len()
            .try_into()
            .expect("more arguments than an i32 can fit"),
        argv: args.as_mut_ptr(),
        allocated: 0,
    };

    unsafe {
        let ret = sys::fuse_opt_parse(&mut args, std::ptr::null_mut(), std::ptr::null_mut(), None);
        if ret == -1 {
            panic!("Failed to parse fuse args");
        }

        sys::fuse_main_real(
            args.argc,
            args.argv,
            &FUSE_CLIENT_OPERATIONS,
            std::mem::size_of_val(&FUSE_CLIENT_OPERATIONS),
            &mut client as *mut FuseClient as *mut c_void,
        );
    }
}
