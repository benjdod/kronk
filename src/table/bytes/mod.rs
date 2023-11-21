use std::mem::size_of;

pub trait FromSlice: Sized {
    type Err;
    fn from_byte_buffer(buf: &[u8]) -> Result<Self, Self::Err>;
}

pub trait ToNativeType<T> {
    type Err;
    fn to_native_type(&self) -> Result<T, Self::Err>;
}

#[derive(Debug, Clone, Copy)]
pub enum NumericTypeConversionError {
    InsufficientByteBufferSize(usize, usize)
}

impl ToNativeType<u32> for [u8] {
    type Err = NumericTypeConversionError;
    fn to_native_type(&self) -> Result<u32, Self::Err> {
        let sized_buf: [u8; 4] = self[..4].try_into()
            .map_err(|_| NumericTypeConversionError::InsufficientByteBufferSize(size_of::<u32>(), self.len()))?;
        Ok(u32::from_le_bytes(sized_buf))
    }
}

impl ToNativeType<i32> for [u8] {
    type Err = NumericTypeConversionError;
    fn to_native_type(&self) -> Result<i32, Self::Err> {
        let sized_buf: [u8; 4] = self[..4].try_into()
            .map_err(|_| NumericTypeConversionError::InsufficientByteBufferSize(size_of::<i32>(), self.len()))?;
        Ok(i32::from_le_bytes(sized_buf))
    }
}

impl ToNativeType<u64> for [u8] {
    type Err = NumericTypeConversionError;
    fn to_native_type(&self) -> Result<u64, Self::Err> {
        let sized_buf: [u8; size_of::<u64>()] = self[..size_of::<u64>()].try_into()
            .map_err(|_| NumericTypeConversionError::InsufficientByteBufferSize(size_of::<u64>(), self.len()))?;
        Ok(u64::from_le_bytes(sized_buf))
    }
}

impl ToNativeType<i64> for [u8] {
    type Err = NumericTypeConversionError;
    fn to_native_type(&self) -> Result<i64, Self::Err> {
        let sized_buf: [u8; size_of::<i64>()] = self[..size_of::<i64>()].try_into()
            .map_err(|_| NumericTypeConversionError::InsufficientByteBufferSize(size_of::<i64>(), self.len()))?;
        Ok(i64::from_le_bytes(sized_buf))
    }
}

impl FromSlice for i32 {
    type Err = NumericTypeConversionError;
    fn from_byte_buffer(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for u32 {
    type Err = NumericTypeConversionError;
    fn from_byte_buffer(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for i64 {
    type Err = NumericTypeConversionError;
    fn from_byte_buffer(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}

impl FromSlice for u64 {
    type Err = NumericTypeConversionError;
    fn from_byte_buffer(buf: &[u8]) -> Result<Self, Self::Err> {
        buf.to_native_type()
    }
}