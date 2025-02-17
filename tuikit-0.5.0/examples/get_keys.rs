use std::time::Duration;
use tuikit::input::KeyBoard;
use tuikit::key::Key;
use tuikit::output::Output;
use tuikit::raw::IntoRawMode;

fn main() {
    let _stdout = std::io::stdout().into_raw_mode().unwrap();
    let mut output = Output::new(Box::new(_stdout)).unwrap();
    output.enable_mouse_support();
    output.flush();

    println!("program will exit on pressing `q` or wait 5 seconds");

    // let mut keyboard = KeyBoard::new(Box::new(std::io::stdin()));
    let mut keyboard = KeyBoard::new_with_tty();
    while let Ok(key) = keyboard.next_key_timeout(Duration::from_secs(5)) {
        if key == Key::Char('q') {
            break;
        }
        println!("print: {:?}", key);
    }
    output.disable_mouse_support();
    output.flush();
}
