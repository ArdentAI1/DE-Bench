import os

# AI Agent task for PostgreSQL Full-Text Search implementation
User_Input = """
Our e-commerce site has a products table with 10,000+ items, but customers are complaining that the basic LIKE search is too slow and doesn't find relevant results. For example, searching "laptop" should also find "MacBook" and searching "red leather jacket" should work as a phrase.

We need you to implement full-text search in PostgreSQL for our product catalog.

Requirements:
1. Search should work across product name, description, brand, category, and tags
2. Product names should be weighted higher than descriptions in search results
3. Support phrase searches like "red leather jacket" 
4. Handle typos and misspellings gracefully
5. Search should be fast (under 100ms) even with our large catalog
6. Keep tsvector updated automatically when products are added or updated
7. We also have a specifications column (JSONB) - include that in search if possible

The products table has columns: id, name, description, category, price, brand, tags (array), specifications (JSONB), popularity_score, sales_count, created_at.

Make sure searches rank by relevance and that the most popular/exact matches show up first.
"""

# Configuration will be generated dynamically by create_config function
