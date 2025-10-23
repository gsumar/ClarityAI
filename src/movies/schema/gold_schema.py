# Gold layer schema - final column names for unified report
gold_column_mapping = {
    # Keep movie_title as is
    'movie_title': 'movie_title',
    
    # Audience data - rename to remove _audience suffix
    'release_year_audience': 'audience_release_year',
    'critic_score_percentage_audience': 'audience_score_percentage',
    'top_critic_score_audience': 'audience_average_score',
    'total_critic_reviews_counted_audience': 'total_audience_ratings',
    
    # Critic data - rename to remove _critic suffix
    'release_year_critic': 'critic_release_year',
    'critic_score_percentage_critic': 'critic_score_percentage',
    'top_critic_score_critic': 'top_critic_score',
    'total_critic_reviews_counted_critic': 'total_critic_reviews_counted',
    
    # Box office data - keep as is
    'release_year': 'release_year',
    'total_box_office_gross_usd': 'total_box_office_gross_usd',
    'production_budget_usd': 'production_budget_usd',
    'marketing_spend_usd': 'marketing_spend_usd',
}

