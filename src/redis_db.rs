use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, SystemTime};

enum RDBOpCodes {
    Eof,
    SelectDB,
    ExpireTime,
    ExpireTimeMs,
    ResizeDB,
    Aux,
}

impl RDBOpCodes {
    fn from_u8(value: &u8) -> Result<RDBOpCodes> {
        match value {
            0xFF => Ok(RDBOpCodes::Eof),
            0xFE => Ok(RDBOpCodes::SelectDB),
            0xFD => Ok(RDBOpCodes::ExpireTime),
            0xFC => Ok(RDBOpCodes::ExpireTimeMs),
            0xFB => Ok(RDBOpCodes::ResizeDB),
            0xFA => Ok(RDBOpCodes::Aux),
            _ => bail!("Invalid RDB opcode {}", value),
        }
    }

    #[allow(dead_code)]
    fn to_u8(&self) -> u8 {
        match self {
            RDBOpCodes::Eof => 0xFF,
            RDBOpCodes::SelectDB => 0xFE,
            RDBOpCodes::ExpireTime => 0xFD,
            RDBOpCodes::ExpireTimeMs => 0xFC,
            RDBOpCodes::ResizeDB => 0xFB,
            RDBOpCodes::Aux => 0xFA,
        }
    }
}

enum RDBLenEncodings {
    SixBit(u64),
    FourteenBit(u64),
    SixtyFourBit(u64),
    SpecialEncoding(u32),
}

impl RDBLenEncodings {
    fn from_u8(bites: &mut impl Iterator<Item = u8>) -> Result<RDBLenEncodings> {
        let first_byte = bites.next().context("Iter reached end")?;
        let first_2_bits = first_byte & 192;
        match first_2_bits {
            0 => Ok(RDBLenEncodings::SixBit(first_byte as u64)),
            64 => {
                let first_6_bits = first_byte & 63;
                let next_byte = bites.next().context("Iter reached end")?;
                let value = ((first_6_bits as u16) << 8) | next_byte as u16;
                Ok(RDBLenEncodings::FourteenBit(value as u64))
            }
            128 => {
                let mut val: u64 = 0;
                for _ in 0..4 {
                    let next_byte = bites.next().context("Iter reached end")?;
                    val = (val << 8) | next_byte as u64;
                }
                Ok(RDBLenEncodings::SixtyFourBit(val))
            }
            192 => {
                let last_6_bits = first_byte & 63;
                if last_6_bits == 0 {
                    let next_byte = bites.next().context("Iter reached end")?;
                    return Ok(RDBLenEncodings::SpecialEncoding(next_byte as u32));
                } else if last_6_bits < 3 {
                    let mut val: u32 = 0;
                    for _ in 0..last_6_bits {
                        let next_byte = bites.next().context("Iter reached end")?;
                        val = (val << 8) | next_byte as u32;
                    }
                    return Ok(RDBLenEncodings::SpecialEncoding(val));
                }

                bail!("Special encoding: {}", last_6_bits);
            }
            _ => bail!("Invalid RDB length encoding"),
        }
    }

    #[allow(dead_code)]
    fn to_string(&self) -> String {
        match self {
            RDBLenEncodings::SixBit(num) => num.to_string(),
            RDBLenEncodings::FourteenBit(num) => num.to_string(),
            RDBLenEncodings::SixtyFourBit(num) => num.to_string(),
            RDBLenEncodings::SpecialEncoding(num) => num.to_string(),
        }
    }
}

enum RDBValueEncodings {
    String,
    // List,
    // Set,
    // SortedSet,
    // Hash,
    // ZipMap,
    // ZipList,
    // IntSet,
    // SortedSetZipList,
    // HashMapZipList,
    // ListQuickList,
}

impl RDBValueEncodings {
    fn from_u8(value: &u8) -> Result<RDBValueEncodings> {
        match value {
            0 => Ok(RDBValueEncodings::String),
            e => bail!("Invalid RDB value encoding {}", e),
        }
    }
}

enum StringEncoding {
    Int32(u32),
    LenPrefixed(LenPrefixedString),
    #[allow(dead_code)]
    LZF,
}

struct LenPrefixedString {
    #[allow(dead_code)]
    len: u32,
    value: String,
}

impl StringEncoding {
    fn from_u8(bites: &mut impl Iterator<Item = u8>) -> Result<StringEncoding> {
        let len_encoding = RDBLenEncodings::from_u8(bites)?;
        match len_encoding {
            RDBLenEncodings::SixBit(num)
            | RDBLenEncodings::FourteenBit(num)
            | RDBLenEncodings::SixtyFourBit(num) => {
                let mut val: Vec<u8> = Vec::new();
                for _ in 0..num {
                    let byte = bites.next().context("Iter reached end")?;
                    val.push(byte);
                }
                let lps = LenPrefixedString {
                    len: num as u32,
                    value: String::from_utf8(val).context("Invalid utf8")?,
                };
                Ok(StringEncoding::LenPrefixed(lps))
            }
            RDBLenEncodings::SpecialEncoding(num) => Ok(StringEncoding::Int32(num)),
        }
    }
    fn to_string(&self) -> String {
        match self {
            StringEncoding::Int32(num) => num.to_string(),
            StringEncoding::LenPrefixed(lps) => lps.value.clone(),
            StringEncoding::LZF => "LZF".to_string(),
        }
    }
}

pub struct RedisDB {
    dir: String,
    file_name: String,
}

impl RedisDB {
    pub fn new(dir: String, file_name: String) -> Self {
        Self { dir, file_name }
    }

    fn get_next_opcode(&self, bite: &u8) -> Result<RDBOpCodes> {
        RDBOpCodes::from_u8(bite)
    }

    fn get_rbd_bytes(&self) -> Result<Vec<u8>> {
        let path = format!("{}/{}", self.dir, self.file_name);
        let mut file = File::open(path).context("Error while opening rdb file")?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .context("Error while reading rdb file")?;
        Ok(buffer)
    }

    fn get_expiry(
        &self,
        next_byte: u8,
        byte_iter: &mut impl Iterator<Item = u8>,
    ) -> Result<Option<SystemTime>> {
        let expiry = match self.get_next_opcode(&next_byte) {
            Err(_) => None,
            Ok(opcode) => match opcode {
                RDBOpCodes::ExpireTime => {
                    let _ = byte_iter.next().context("Iter reached end")?;
                    let arr = byte_iter.take(4).collect::<Vec<u8>>();
                    let expiry = u64::from_le_bytes(arr.try_into().unwrap());
                    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(expiry))
                }
                RDBOpCodes::ExpireTimeMs => {
                    let _ = byte_iter.next().context("Iter reached end")?;
                    let arr = byte_iter.take(8).collect::<Vec<u8>>();
                    let expiry = u64::from_le_bytes(arr.try_into().unwrap());
                    SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(expiry))
                }
                _ => None,
            },
        };
        Ok(expiry)
    }

    pub fn read_rdb(&mut self) -> Result<(HashMap<String, String>, HashMap<String, SystemTime>)> {
        let mut bytes = self.get_rbd_bytes()?;
        let magic_string = bytes.drain(0..5).collect::<Vec<u8>>();
        if magic_string != b"REDIS" {
            bail!("Invalid RDB file");
        }
        let _version = bytes.drain(0..4).collect::<Vec<u8>>();
        let mut byte_iter = bytes.into_iter().peekable();
        let mut next_byte = byte_iter.next().context("Iter reached end")?;

        let mut kivals: HashMap<String, String> = HashMap::new();
        let mut exp_map: HashMap<String, SystemTime> = HashMap::new();

        #[allow(irrefutable_let_patterns)]
        while let opcode = self.get_next_opcode(&next_byte)? {
            match opcode {
                RDBOpCodes::Eof => {
                    return Ok((kivals, exp_map));
                }
                RDBOpCodes::SelectDB => {
                    let _db_number = RDBLenEncodings::from_u8(&mut byte_iter)?;
                    let opcode =
                        self.get_next_opcode(&byte_iter.next().context("Iter reached end")?)?;
                    if let RDBOpCodes::ResizeDB = opcode {
                    } else {
                        bail!("Invalid RDB opcode lol")
                    }
                    let _db_size = RDBLenEncodings::from_u8(&mut byte_iter)?;
                    let _exp_size = RDBLenEncodings::from_u8(&mut byte_iter)?;

                    loop {
                        let peeked_byte = byte_iter.peek().context("Iter reached end")?.clone();
                        let expiry_arg = self.get_expiry(peeked_byte, &mut byte_iter)?;
                        let (k, v) = self.load_key_val(&mut byte_iter)?;
                        kivals.insert(k.clone(), v);
                        if let Some(expiry) = expiry_arg {
                            exp_map.insert(k, expiry);
                        }
                        if let Some(next_byte) = byte_iter.peek() {
                            match self.get_next_opcode(&next_byte) {
                                Ok(opcode) => match opcode {
                                    RDBOpCodes::SelectDB
                                    | RDBOpCodes::Aux
                                    | RDBOpCodes::ResizeDB
                                    | RDBOpCodes::Eof => break,
                                    _ => continue,
                                },
                                Err(_) => continue,
                            }
                        }
                    }
                }
                RDBOpCodes::Aux => loop {
                    let key_string_encoding = StringEncoding::from_u8(&mut byte_iter)?;
                    let _key = key_string_encoding.to_string();
                    let val_string_encoding = StringEncoding::from_u8(&mut byte_iter)?;
                    let _val = val_string_encoding.to_string();
                    let nb = byte_iter.peek().context("Iter reached end")?;
                    if let RDBOpCodes::SelectDB =
                        self.get_next_opcode(&nb).unwrap_or(RDBOpCodes::Aux)
                    {
                        break;
                    }
                    if let RDBOpCodes::Aux =
                        self.get_next_opcode(&nb).unwrap_or(RDBOpCodes::SelectDB)
                    {
                        byte_iter.next().context("Iter reached end")?;
                        continue;
                    }
                },
                RDBOpCodes::ResizeDB => bail!("ResizeDB should come after select DB"),
                RDBOpCodes::ExpireTime => bail!("ExpireTime should come after select DB"),
                RDBOpCodes::ExpireTimeMs => bail!("ExpireTimeMs should come after select DB"),
            }
            next_byte = byte_iter.next().context("Iter reached end")?;
        }

        bail!("End of file not found");
    }

    fn load_key_val(&mut self, bites: &mut impl Iterator<Item = u8>) -> Result<(String, String)> {
        let val_type_byte = bites.next().context("Iter reached end")?;
        let val_encoding = RDBValueEncodings::from_u8(&val_type_byte)?;
        let key_string_encoding = StringEncoding::from_u8(bites)?;
        let key = key_string_encoding.to_string();
        match val_encoding {
            RDBValueEncodings::String => {
                let val_string_encoding = StringEncoding::from_u8(bites)?;
                let val = val_string_encoding.to_string();
                Ok((key, val))
            }
        }
    }
}
