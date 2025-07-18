pub mod helper {
    #[macro_export]
    macro_rules! make_continue {
        ($res:expr) => {
            match $res {
                Ok(val) => val,
                Err(e) => {
                    println!("error {:?}", e);
                    continue;
                }
            }
        };

        ($res:expr, $label:tt) => {
            match $res {
                Ok(val) => val,
                Err(_) => {
                    continue $label;
                }
            }
        };
    }

    pub use make_continue;
}
