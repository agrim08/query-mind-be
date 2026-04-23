import asyncio
from google import genai
from google.genai import types as genai_types

from app.core.config import settings
from app.schemas.design import DBSchemaDesign

MODEL_NAME = "gemini-2.5-flash"

SYSTEM_PROMPT = """You are an expert Database Architect.
Listen to the user's requirements for an application, and design a robust PostgreSQL relational database schema.
You MUST output structured JSON matching the provided schema. 

Guidelines:
1. Tables must have an 'id' (typically UUID or SERIAL) as a primary key.
2. If two tables are related, include a foreign key column in the child table (e.g. `user_id`) and set `isForeign: true`.
3. Add edges representing the relationships, using the table IDs as source and target.
4. Provide appropriate SQL types (VARCHAR(255), TIMESTAMP, INTEGER, BOOLEAN, etc.) and constraints (NOT NULL, UNIQUE).
"""

async def generate_schema_from_prompt(prompt: str) -> DBSchemaDesign:
    """
    Generate a complete visual DB schema from a natural language prompt using GenAI.
    Enforces extraction into the DBSchemaDesign Pydantic schema structure.
    """
    client = genai.Client(api_key=settings.GOOGLE_API_KEY)
    
    # Run the generation with structured output configuration
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=0.2,
                response_mime_type="application/json",
                response_schema=DBSchemaDesign,
            ),
        ),
    )
    
    # response.parsed contains the typed parsed object!
    if not hasattr(response, "parsed") or not response.parsed:
        # Fallback if parsed is missing or None
        import json
        data = json.loads(response.text)
        return DBSchemaDesign(**data)
        
    return response.parsed
