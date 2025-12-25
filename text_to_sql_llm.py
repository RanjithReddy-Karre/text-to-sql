from openai import OpenAI
from config import operrouter

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key= operrouter.API_KEY
)

prompt = """
You are an IPL analytics SQL assistant.

Rules:
- Use ONLY the table and columns provided.
- Do NOT invent tables or columns.
- Exclude matches where is_completed_match = FALSE.
- Return ONLY a valid Snowflake SQL query.
- Do not Explain the SQL.

Additional rules:
- Group only by the dimensions explicitly mentioned in the question.
- Do NOT use HAVING unless explicitly requested.
- Name columns accurately (counts vs percentages).
- Try to provide results in percentages along with counts whereever necessary but not mandatory.

Schema:
Table: matches_analytics

Description:
One row per IPL match. This table contains match-level context and normalized
match outcomes. It does NOT contain scores.

Use this table for:
- Match counts
- Toss impact analysis
- Venue and city analysis
- Match outcome analysis
- League vs playoff comparisons

Important columns:
- match_id: unique identifier for a match
- season: IPL season year
- match_date: date of the match
- city: city where the match was played
- venue_name: stadium name
- home_team_name: home team
- away_team_name: away team

Toss and innings context:
- toss_winner_team: team that won the toss
- first_batting_team: team batting first
- second_batting_team: team batting second

Match outcome columns:
- match_result_type: NORMAL, SUPER_OVER, NO_RESULT, ABANDONED
- winner_team: team that won the match (NULL if no winner)
- is_completed_match: TRUE if match had a valid result
- win_margin_type: RUNS or WICKETS
- win_margin_value: margin of victory
- match_type: League, Qualifier 1, Eliminator, Qualifier 2, Final


Question:
Has the impact of the toss increased or decreased over IPL seasons?
"""

# completion = client.chat.completions.create(
#     model="kwaipilot/kat-coder-pro:free",
#     messages=[{"role": "user", "content": prompt}],
#     extra_body={
#         "provider": {
#             "order": ["streamlake/fp16"],
#             "allow_fallbacks": False,
#             "require_parameters": True
#         }
#     }
# )

completion = client.chat.completions.create(
    model="kwaipilot/kat-coder-pro:free",
    messages=[{"role": "user", "content": prompt}],
    temperature=0.0,
    top_p=1.0,
    frequency_penalty=0.0,
    presence_penalty=0.0,
    extra_body={
        "provider": {
            "order": ["streamlake/fp16"],
            "allow_fallbacks": False
        }
    }
)



sql_query = completion.choices[0].message.content
print(sql_query)
