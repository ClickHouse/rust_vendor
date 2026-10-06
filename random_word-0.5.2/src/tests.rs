#[cfg(test)]
mod tests {
    use crate::*;
    use ahash::AHashSet;

    const ITERATIONS: usize = 1000;

    #[test]
    #[cfg(feature = "de")]
    fn test_german_randomness() {
        test_language_randomness(Lang::De);
    }

    #[test]
    #[cfg(feature = "en")]
    fn test_english_randomness() {
        test_language_randomness(Lang::En);
    }

    #[test]
    #[cfg(feature = "es")]
    fn test_spanish_randomness() {
        test_language_randomness(Lang::Es);
    }

    #[test]
    #[cfg(feature = "fr")]
    fn test_french_randomness() {
        test_language_randomness(Lang::Fr);
    }

    #[test]
    #[cfg(feature = "ja")]
    fn test_japanese_randomness() {
        test_language_randomness(Lang::Ja);
    }

    #[test]
    #[cfg(feature = "ru")]
    fn test_russian_randomness() {
        test_language_randomness(Lang::Ru);
    }

    #[test]
    #[cfg(feature = "zh")]
    fn test_chinese_randomness() {
        test_language_randomness(Lang::Zh);
    }

    fn test_language_randomness(lang: Lang) {
        let words = all(lang);
        let total_words = words.len();

        assert!(total_words >= 100000, "Less than 100,000 words available for {:?}", lang);

        let mut seen: AHashSet<&'static str> = AHashSet::new();
        let mut consecutive_duplicates = 0;
        let mut max_consecutive_duplicates = 0;
        let mut last_word = "";

        // Test basic randomness and uniqueness
        for i in 0..ITERATIONS / 3 {
            let word = get(lang);

            // Basic validation
            assert!(!word.is_empty(), "Empty word found for {:?} at iteration {}", lang, i);
            assert!(words.contains(&word), "Word '{}' not found in dictionary for {:?}", word, lang);

            seen.insert(word);

            // Track consecutive duplicates
            if word == last_word {
                consecutive_duplicates += 1;
                max_consecutive_duplicates = max_consecutive_duplicates.max(consecutive_duplicates);
            } else {
                consecutive_duplicates = 0;
            }
            last_word = word;
        }

        // Statistical analysis
        let unique_words_seen = seen.len();
        let duplicate_count = ITERATIONS - unique_words_seen;
        let duplicate_ratio = duplicate_count as f64 / ITERATIONS as f64;

        eprintln!("{:?} - Total words: {}, Iterations: {}, Unique seen: {}, Duplicates: {:.2}%, Max consecutive: {}",
                 lang, total_words, ITERATIONS, unique_words_seen, duplicate_ratio * 100.0, max_consecutive_duplicates);

        // Assertions for randomness quality
        assert!(unique_words_seen > ITERATIONS / 10,
                "Poor randomness: only {} unique words out of {} iterations for {:?}",
                unique_words_seen, ITERATIONS, lang);

        assert!(max_consecutive_duplicates < 3,
                "Too many consecutive duplicates: {} for {:?}",
                max_consecutive_duplicates, lang);

        // For languages with sufficient words, test distribution
        if total_words > 1000 {
            test_word_distribution(lang, 5000);
        }

        // Test hamming distance for short words
        test_hamming_distance_distribution(lang, 1000);
    }

    fn test_word_distribution(lang: Lang, sample_size: usize) {
        let words = words::get(lang);
        let mut word_frequency = ahash::AHashMap::new();

        for _ in 0..sample_size {
            let word = get(lang);
            *word_frequency.entry(word).or_insert(0) += 1;
        }

        let expected_frequency = sample_size as f64 / words.len() as f64;
        let mut underrepresented = 0;
        let mut chi_squared = 0.0;
        for word in words.iter() {
            let count = word_frequency.get(word).copied().unwrap_or(0);
            let deviation = (count as f64 - expected_frequency).abs();
            chi_squared += (deviation * deviation) / expected_frequency;
        }

        // Chi-squared test for uniform distribution (simplified)
        let chi_squared_per_word = chi_squared / words.len() as f64;

        eprintln!("{:?} - Distribution chi-squared per word: {:.3}", lang, chi_squared_per_word);

        assert!(chi_squared_per_word < 2.0,
                "Poor distribution for {:?}: chi-squared per word = {:.3}",
                lang, chi_squared_per_word);

        assert!(underrepresented < words.len() / 10,
                "Too many underrepresented words in {:?}: {} out of {}",
                lang, underrepresented, words.len());
    }

    fn test_hamming_distance_distribution(lang: Lang, iterations: usize) {
        let mut last_word = get(lang);
        let mut hamming_distances = Vec::new();
        let mut same_word_count = 0;

        for _ in 0..iterations {
            let current_word = get(lang);

            if current_word == last_word {
                same_word_count += 1;
            } else {
                let distance = hamming_distance(last_word, current_word);
                hamming_distances.push(distance);
            }

            last_word = current_word;
        }

        if !hamming_distances.is_empty() {
            let avg_distance = hamming_distances.iter().sum::<usize>() as f64 / hamming_distances.len() as f64;
            let min_distance = *hamming_distances.iter().min().unwrap_or(&0);
            let max_distance = *hamming_distances.iter().max().unwrap_or(&0);

            eprintln!("{:?} - Hamming distance: avg={:.2}, min={}, max={}, same={}",
                     lang, avg_distance, min_distance, max_distance, same_word_count);

            // For languages with meaningful words, expect some diversity in hamming distance
            if avg_distance > 2.0 {
                assert!(min_distance > 0, "No character differences found in {:?}", lang);
                assert!(max_distance >= 3, "Insufficient character variation in {:?}", lang);
            }
        }
    }

    fn hamming_distance(a: &str, b: &str) -> usize {
        let char_diff = a.chars()
            .zip(b.chars())
            .filter(|(c1, c2)| c1 != c2)
            .count();
        char_diff + a.len().abs_diff(b.len())
    }
}