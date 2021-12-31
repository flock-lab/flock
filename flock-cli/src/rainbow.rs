// Copyright (c) 2020-present, UMD Database Group.
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

//! A simple rainbow-colored logger.

use std::f64::consts::PI;

/// Prints the text in the rainbow fansion.
pub fn rainbow_println(line: &str) {
    let frequency: f64 = 0.1;
    let spread: f64 = 3.0;
    for (i, c) in line.char_indices() {
        let (r, g, b) = rgb(frequency, spread, i as f64);
        if c == ' ' {
            print!("{}", c);
        } else {
            print!("\x1b[38;2;{};{};{}m{}\x1b[0m", r, g, b, c);
        }
    }
    println!();
}

/// Generates RGB for rainbow print.
fn rgb(freq: f64, spread: f64, i: f64) -> (u8, u8, u8) {
    let j = i / spread;
    let red = (freq * j + 0.0).sin() * 127.0 + 128.0;
    let green = (freq * j + 2.0 * PI / 3.0).sin() * 127.0 + 128.0;
    let blue = (freq * j + 4.0 * PI / 3.0).sin() * 127.0 + 128.0;

    (red as u8, green as u8, blue as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rainbow_print() {
        let text = include_str!("./flock");
        rainbow_println(text);
    }
}
