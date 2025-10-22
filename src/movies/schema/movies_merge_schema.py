schema = {
    "movie_title": "string",
    "release_year": "int64",
    "critic_score_percentage": "int64",
    "top_critic_score": "float64",
    "total_critic_reviews_counted": "int64",
}

# Column mappings for different data providers
audience_pulse_mapping = {
    'title': 'movie_title',
    'year': 'release_year',
    'audience_average_score': 'critic_score_percentage',
    'total_audience_ratings': 'top_critic_score',
    'domestic_box_office_gross': 'total_critic_reviews_counted'
}

critic_agg_mapping = {
    # CriticAgg already has correct column names
}
