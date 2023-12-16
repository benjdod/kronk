const KRONKSTORE_DIRECTORY: &str = "./.kronkstore";
const KRONKSTORE_TABLES_DIR: &str = "./.kronkstore/tables";

fn talk_to_filesystem() -> std::io::Result<()> {
    std::fs::create_dir(KRONKSTORE_DIRECTORY).or_else(|e| match e.kind() {
        std::io::ErrorKind::AlreadyExists => Ok(()),
        _ => Err(e)
    })?;
    let hello_pb = Path::new(KRONKSTORE_DIRECTORY).join("hello.txt");
    let mut f = File::create(hello_pb)?;
    f.write_all(b"hello my friend!")?;
    Ok(())
}