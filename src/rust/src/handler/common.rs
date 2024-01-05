use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};

pub fn new_id() -> i64 {
    struct Filter<Dist, Test> {
        dist: Dist,
        test: Test,
    }

    impl<T, Dist, Test> Distribution<T> for Filter<Dist, Test>
    where
        Dist: Distribution<T>,
        Test: Fn(&T) -> bool,
    {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
            loop {
                let x = self.dist.sample(rng);
                if (self.test)(&x) {
                    return x;
                }
            }
        }
    }

    let mut rng = thread_rng();
    let dist = Filter {
        dist: Uniform::new(i64::MIN, i64::MAX),
        test: |x: &_| (x != &0),
    };

    rng.sample(&dist)
}
