// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! A bitmap is a bit vector that can be used to represent a set of integers.
//! It is a simple wrapper around a `Vec<u8>` that provides a few convenience
//! methods to manipulate the bits.
//!
//! The goal of this module is to provide a fast, compact, and memory-efficient
//! representation to track which data fragments in the current time-window are
//! available, versus which are not.

use arrow::buffer::MutableBuffer;
use arrow::util::bit_util;

/// A bitmap is a bit vector that can be used to represent a set of flags.
#[derive(Debug)]
pub struct Bitmap {
    /// A [`MutableBuffer`] is Arrow's interface to build a [`Buffer`] out of
    /// items or slices of items.
    pub bits: MutableBuffer,
}

impl Bitmap {
    /// Creates a new bitmap with the given capacity.
    pub fn new(num_bits: usize) -> Self {
        let num_bytes = num_bits / 8 + if num_bits % 8 > 0 { 1 } else { 0 };
        let r = num_bytes % 64;
        let len = if r == 0 {
            num_bytes
        } else {
            num_bytes + 64 - r
        };

        let mut buf = MutableBuffer::new(len);
        buf.extend_from_slice(&vec![0x00; len]);
        Bitmap { bits: buf }
    }

    /// Returns `true` if the bit is set, `false` otherwise.
    pub fn is_set(&self, i: usize) -> bool {
        assert!(i < (self.bits.len() << 3));
        unsafe { bit_util::get_bit_raw(self.bits.as_ptr(), i) }
    }

    /// Sets the bit at the given index.
    pub fn set(&mut self, i: usize) {
        assert!(i < (self.bits.len() << 3));
        unsafe {
            bit_util::set_bit_raw(self.bits.as_mut_ptr(), i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn test_bitmap() -> Result<()> {
        let mut bitmap = Bitmap::new(1024);
        (0..1024).for_each(|i| assert_eq!(false, bitmap.is_set(i)));

        bitmap.set(0);
        assert_eq!(true, bitmap.is_set(0));

        bitmap.set(100);
        assert_eq!(true, bitmap.is_set(100));

        Ok(())
    }
}
