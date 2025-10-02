use bridge_core::offsets::OffsetRecord;

fn main() {
    let offset = OffsetRecord::new("test", "testing", 0, 1);
    println!("{:?}", offset);
}
