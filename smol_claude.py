from smolagents import CodeAgent, ToolCallingAgent, DuckDuckGoSearchTool, LiteLLMModel, PythonInterpreterTool, tool
from typing import Optional
import os

from dotenv import load_dotenv
load_dotenv( '.env')

anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
if not anthropic_api_key:
    raise ValueError("ANTHROPIC_API_KEY environment variable is required")

model = LiteLLMModel(model_id="claude-3-5-sonnet-20240620",
                     api_key=anthropic_api_key)


@tool
def get_weather(team: str) -> str:
    """
    Retorne os 3 próximos jogos de um time de futebol.
    Args:
        team: o time de futebol
    """
    return f"Os 3 próximos jogos do {team} serão contra Santos (casa), Água Branca (fora) e Mirassol (fora)."

# agent = ToolCallingAgent(tools=[get_weather], model=model)


agent = CodeAgent(tools=[DuckDuckGoSearchTool()], model=model)
answer = agent.run("Quais serão os próximos jogos do Corinthians?")
print(answer)