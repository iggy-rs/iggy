#[derive(Default)]
pub struct HeadTailBuffer<T>
where
    T: Default + Eq + Copy,
{
    data: [T; 2],
    head_index: usize,
}

impl<T> HeadTailBuffer<T>
where
    T: Default + Eq + Copy,
{
    pub fn new() -> Self {
        HeadTailBuffer {
            data: [Default::default(); 2],
            head_index: 0,
        }
    }

    pub fn push(&mut self, element: T) {
        self.data[(self.head_index + 1) % 2] = element;
        self.head_index = (self.head_index + 1) % 2;
    }

    pub fn tail(&self) -> Option<T> {
        if self.head_index == 0 && self.data[1] == Default::default() {
            None
        } else {
            // If the tail is empty return None
            if self.data[self.head_index] == Default::default() {
                return None;
            }
            Some(self.data[(self.head_index + 1) % 2])
        }
    }
}
