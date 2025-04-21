import re
from typing import Optional, Any

from google.adk.agents import Agent
from dotenv import load_dotenv
import os

from neo4j import GraphDatabase
import logging

logger = logging.getLogger('agent_neo4j_cypher')
logger.info("Initializing Database for tools")

load_dotenv('.env')

NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

class neo4jDatabase:
    def __init__(self,  neo4j_uri: str, neo4j_username: str, neo4j_password: str):
        """Initialize connection to the Neo4j database"""
        d = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))
        d.verify_connectivity()
        self.driver = d

    def is_write_query(self, query: str) -> bool:
      return re.search(r"\b(MERGE|CREATE|SET|DELETE|REMOVE|ADD)\b", query, re.IGNORECASE) is not None

    #TODO: Use result transformer here to r: r.data()
    def _execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a Cypher query and return results as a list of dictionaries"""
        logger.debug(f"Executing query: {query}")
        try:
            if self.is_write_query(query):
                logger.error(f"Write query not supported {query}")
                raise "Write Queries are not supported in this agent"
                # logger.debug(f"Write query affected {counters}")
                # result = self.driver.execute_query(query, params)
                # counters = vars(result.summary.counters)
                # return [counters]
            else:
                #TODO: Add Routing here for effciency
                result = self.driver.execute_query(query, params)
                results = [dict(r) for r in result.records]
                logger.debug(f"Read query returned {len(results)} rows")
                return results
        except Exception as e:
            logger.error(f"Database error executing query: {e}\n{query}")
            raise

db = neo4jDatabase(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

def get_schema() -> list[dict[str,Any]]:
  """Get the schema of the database, returns node-types(labels) with their types and attributes and relationships between node-labels
  Args: None
  Returns:
    list[dict[str,Any]]: A list of dictionaries representing the schema of the database
    For example
    ```
    [{'label': 'Person','attributes': {'summary': 'STRING','id': 'STRING unique indexed', 'name': 'STRING indexed'},
      'relationships': {'HAS_PARENT': 'Person', 'HAS_CHILD': 'Person'}}]
    ```
  """
  try:
      results = db._execute_query(
              """
call apoc.meta.data() yield label, property, type, other, unique, index, elementType
where elementType = 'node' and not label starts with '_'
with label,
collect(case when type <> 'RELATIONSHIP' then [property, type + case when unique then " unique" else "" end + case when index then " indexed" else "" end] end) as attributes,
collect(case when type = 'RELATIONSHIP' then [property, head(other)] end) as relationships
RETURN label, apoc.map.fromPairs(attributes) as attributes, apoc.map.fromPairs(relationships) as relationships
              """
          )
      return results
  except Exception as e:
      return [{"error":str(e)}]

def execute_read_query(query: str, params: Optional[dict[str, Any]]=None) -> list[dict[str, Any]]:
    """
    Execute a Neo4j Cypher query and return results as a list of dictionaries
    Args:
        query (str): The Cypher query to execute
        params (dict[str, Any], optional): The parameters to pass to the query or None.
    Raises:
        Exception: If there is an error executing the query
    Returns:
        list[dict[str, Any]]: A list of dictionaries representing the query results
    """
    try:
        if params is None:
            params = {}
        results = db._execute_query(query, params)
        return results
    except Exception as e:
        return [{"error":str(e)}]

def get_all_product_dependencies_on_supplier(supplier_code: str) -> list[dict[str, Any]]:
    """
    Get all the Products that depend on components from a supplier.
    Provides product SkuIds plus the depency chain on components leading up to those supplied from the supplier.
    Args:
        supplier_code (str): code uniquely identifying the supplier
    Returns:
        list[dict[str, Any]]: product SkuIds plus the depency chain on components leading up to those supplied from the supplier.
    """
    try:
        results = db._execute_query("""
        MATCH (s:Supplier {code: $code})
        MATCH path = (p:Item)<-[:BOM*]-(comp:Item)-[:AT]->(s:Supplier)
        RETURN nodes(path)[0].sku_id AS sku_id,
            nodes(path)[0].type AS type,
        collect(apoc.text.join([ n IN nodes(path) | coalesce(n.sku_id, n.code) + '(' + coalesce(n.type, n.tier) + ')' ], ' <- ')) AS supplyChainDependencies
        """, {"code":supplier_code})
        return results
    except Exception as e:
        return [{"error":str(e)}]


def get_supplier_substitutions(supplier_code: str) -> list[dict[str, Any]]:
    """
    Get all all products and components from a supplier along with alternative suppliers to substitute them with
    Args:
        supplier_code (str): code uniquely identifying the supplier
    Returns:
        list[dict[str, Any]]: product and component SkuIds supplied by the given supplier with alternative suppliers that provide them
    """
    try:
        results = db._execute_query("""
        MATCH (s:Supplier {code: $code})
        MATCH (s)<-[:AT]-(c:Item)-[:AT]->(sAlt:Supplier)
        WHERE s <> sAlt
        RETURN c.sku_id AS sku_id,
            c.type AS type,
            collect({SupplierCode: sAlt.code , supplierTier: sAlt.tier, supplierSubType: sAlt.sub_type}) AS AltSuppliers
        """, {"code":supplier_code})
        return results
    except Exception as e:
        return [{"error":str(e)}]

MODEL="gemini-2.5-pro-preview-03-25"

database_agent = Agent(
    model=MODEL,
    name='graph_database_agent',
    instruction="""
      You are an Neo4j graph database and Cypher query expert, that must use the database schema with a user question and repeatedly generate valid cypher statements
      to execute on the database and answer the user's questions in a friendly manner in natural language.
      If in doubt the database schema is always prioritized when it comes to nodes-types (labels) or relationship-types or property names, never take the user's input at face value.
      If the user requests also render tables, charts or other artifacts with the query results.
      Always validate the correct node-labels at the end of a relationship based on the schema.

      If a query fails or doesn't return data, use the error response 3 times to try to fix the generated query and re-run it, don't return the error to the user.
      If you cannot fix the query, explain the issue to the user and apologize.

      Fetch the graph database schema first and keep it in session memory to access later for query generation.
      Keep results of previous executions in session memory and access if needed, for instance ids or other attributes of nodes to find them again
      removing the need to ask the user. This also allows for generating shorter, more focused and less error-prone queries
      to for drill downs, sequences and loops.
      If possible resolve names to primary keys or ids and use those for looking up entities.
      The schema always indicates *outgoing* relationship-types from an entity to another entity, the graph patterns read like english language.
      `company has supplier` would be the pattern `(o:Organization)-[:HAS_SUPPLIER]->(s:Organization)`

      To get the schema of a database use the `get_schema` tool without parameters. Store the response of the schema tool in session context
      to access later for query generation.

      To answer a user question generate one or more Cypher statements based on the database schema and the parts of the user question.
      If necessary resolve categorical attributes (like names, countries, industries, publications) first by retrieving them for a set of entities to translate from the user's request.
      Use the `execute_query` tool repeatedly with the Cypher statements, you MUST generate statements that use named query parameters with `$parameter` style names
      and MUST pass them as a second dictionary parameter to the tool, even if empty.
      Parameter data can come from the users requests, prior query results or additional lookup queries.
      After the data for the question has been sufficiently retrieved, pass the data and control back to the parent agent.
    """,
    tools=[
        get_schema, execute_read_query
    ]
)

bom_supplier_research_agent = Agent(
    model=MODEL,
    name='bom_supplier_research_agent',
    instruction="""
    You are an agent that has access to a database of bill of materials (BOM), supplier, product/component, and customer relationships.
    Use the provided tools to answer questions. 
    when returning ifnromation, try to always return not just the factual attribute data but also
    codes, skus, and ids to allow the other agents to investigate them more.
    """,
    tools=[
        get_schema, get_all_product_dependencies_on_supplier, get_supplier_substitutions
    ]
)


root_agent = Agent(
    model=MODEL,
    name='bom_supplier_agent',
    global_instruction = "",
    instruction="""
    You are an agent that has access to a database of bill of materials (BOM), supplier, product/component, and customer relationships.
    You have a set of agents to retrieve information from that knowledge graph, if possible prefer the research agents over the database agent.
    If the user requests it, do render tables, charts or other artifacts with the research results.
    """,

    sub_agents=[bom_supplier_research_agent, database_agent]
)