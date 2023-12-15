use std::mem::size_of;

pub trait FromSlice: Sized {
    type Err;
    fn from_slice(buf: &[u8]) -> Result<Self, Self::Err>;
}

pub trait ToNativeType<T> {
    type Err;
    fn to_native_type(&self) -> Result<T, Self::Err>;
}

#[derive(Debug, Clone, Copy)]
pub enum SizedTypeConversionError {
    InsufficientByteBufferSize(usize, usize)
}
fn to_native_type<T, const SZ: usize>(buf: &[u8], to_type: fn ([u8; SZ]) -> T) -> Result<T, SizedTypeConversionError> where T : Sized {
    let o = size_of::<T>();
    if buf.len() < SZ {
        Err(SizedTypeConversionError::InsufficientByteBufferSize(SZ, buf.len()))
    } else {
        Ok(to_type(<[u8; SZ]>::try_from(&buf[..o]).unwrap()))
    }
}

impl ToNativeType<i32> for [u8] {
    type Err = SizedTypeConversionError;
    fn to_native_type(&self) -> Result<i32, Self::Err> {
        to_native_type::<i32, 4>(self, |b| i32::from_le_bytes(b))
    }
}

impl ToNativeType<u32> for [u8] {
    type Err = SizedTypeConversionError;
    fn to_native_type(&self) -> Result<u32, Self::Err> {
        to_native_type::<u32, 4>(self, |b| u32::from_le_bytes(b))
    }
}

impl ToNativeType<u64> for [u8] {
    type Err = SizedTypeConversionError;
    fn to_native_type(&self) -> Result<u64, Self::Err> {
        to_native_type::<u64, 8>(self, |b| u64::from_le_bytes(b))
    }
}

impl ToNativeType<i64> for [u8] {
    type Err = SizedTypeConversionError;
    fn to_native_type(&self) -> Result<i64, Self::Err> {
        to_native_type::<i64, 8>(self, |b| i64::from_le_bytes(b))
    }
}

impl FromSlice for i32 {
    type Err = SizedTypeConversionError;
    fn from_slice(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for u32 {
    type Err = SizedTypeConversionError;
    fn from_slice(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for i64 {
    type Err = SizedTypeConversionError;
    fn from_slice(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for u64 {
    type Err = SizedTypeConversionError;
    fn from_slice(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}