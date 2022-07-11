/**
 * Make an iterator that aggregates elements from each of the iterables.
 * Returns an iterator of tuples, where the i-th tuple contains the i-th
 * element from each of the argument sequences or iterables. The iterator
 * stops when the shortest input iterable is exhausted.
 *
 * With a single iterable argument, it returns an iterator of 1-tuples.
 * With no arguments, it returns an empty iterator.
 */
export const zip = <T>(...arrays: Array<T[]>): T[][] => {
  const length = Math.min(...arrays.map((arr) => arr.length));
  return Array.from({ length }, (_value, index) =>
    arrays.map((array) => array[index])
  );
};

/**
 * Make an iterator that aggregates elements from each of the iterables.
 * If the iterables are of uneven length, missing values are filled-in
 * with `placeholder`.
 *
 * Iteration continues until the longest iterable is exhausted.
 */
export const zipLongest = <T>(placeholder: T, ...arrays: Array<T[]>): T[][] => {
  const length = Math.max(...arrays.map((arr) => arr.length));
  return Array.from({ length }, (_value, index) =>
    arrays.map((array) =>
      array.length - 1 >= index ? array[index] : placeholder
    )
  );
};
