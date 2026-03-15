import os
from crewai import Agent, LLM
from layer4.tools import chroma_retriever_tool, rules_engine_tool


def get_llm():
    return LLM(
    model="groq/llama-3.1-8b-instant",
    api_key=os.environ.get("GROQ_API_KEY"),
)


def build_agents():
    llm = get_llm()

    lead_analyst = Agent(
        role="Senior Lead Analyst",
        goal=(
            "Analyse a scored lead and produce a concise summary of the key signals "
            "that explain why this lead scored the way it did."
        ),
        backstory=(
            "You are a seasoned GTM analyst who has reviewed thousands of leads. "
            "You know how to cut through noise and surface the signals that actually "
            "matter — score, engagement patterns, and anything notable in call transcripts."
        ),
        tools=[chroma_retriever_tool],
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )

    decision_agent = Agent(
        role="GTM Decision Strategist",
        goal=(
            "Take the analyst's lead summary and produce a clear routing decision: "
            "priority level, urgency, and which rep tier should handle this lead."
        ),
        backstory=(
            "You are a GTM strategist who applies both hard business rules and nuanced "
            "judgment. You know that a rule saying 'score > 80 = high priority' is a "
            "starting point, not the whole picture — context from transcripts and "
            "behaviour can override the raw score."
        ),
        tools=[rules_engine_tool],
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )

    formatter = Agent(
        role="Structured Output Formatter",
        goal=(
            "Convert the routing decision into a clean, valid JSON object. "
            "No interpretation, no added reasoning — just precise formatting."
        ),
        backstory=(
            "You are a meticulous formatter. You receive plain-language decisions "
            "and convert them into consistent, machine-readable JSON every time. "
            "You never add fields that weren't in the input and never omit required ones."
        ),
        tools=[],
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )

    return lead_analyst, decision_agent, formatter