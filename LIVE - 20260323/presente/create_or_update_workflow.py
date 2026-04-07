import json
import base64
from pathlib import Path

from databricks.sdk import WorkspaceClient

# ImportFormat pode variar por versão do SDK; deixo um fallback.
try:
    from databricks.sdk.service.workspace import ImportFormat
    _IMPORT_FMT = ImportFormat.JUPYTER
except Exception:
    _IMPORT_FMT = "JUPYTER"


def import_notebook(w: WorkspaceClient, ipynb_path: Path, workspace_path: str, overwrite: bool = True):
    """
    Importa um .ipynb para o Workspace.
    - Usa o SDK para autenticação (sem host/token manual).
    - Conteúdo vai em base64, format=JUPYTER.
    """
    content_b64 = base64.b64encode(ipynb_path.read_bytes()).decode("utf-8")
    w.workspace.import_(
        path=workspace_path,
        format=_IMPORT_FMT,
        content=content_b64,
        overwrite=overwrite,
    )


def mkdirs_if_needed(w: WorkspaceClient, folder: str):
    # Workspace API cria recursivamente (idempotente).
    w.workspace.mkdirs(folder)


def find_job_id_by_name(w: WorkspaceClient, job_name: str) -> int | None:
    """
    Procura job por nome.
    Uso via endpoint /api/2.1/jobs/list (via api_client do SDK)
    para evitar depender de assinatura específica do método list() do SDK.
    """
    resp = w.api_client.do("GET", "/api/2.1/jobs/list", query={"limit": 100})
    for j in resp.get("jobs", []):
        if j.get("settings", {}).get("name") == job_name:
            return j.get("job_id")
    return None


def build_job_settings(cfg: dict) -> dict:
    """
    Monta payload Jobs API 2.1 multi-task.

    Importante (Free Edition / serverless):
    - Para notebook_task, dá para OMITIR compute/cluster config e deixar serverless.
      (Sem existing_cluster_id / job_clusters / new_cluster)
    """
    folder = cfg["workspace_folder"].rstrip("/")
    nb_map = {n["file"]: f"{folder}/{n['path']}" for n in cfg["notebooks"]}

    src_schema = cfg["src_schema"]
    tgt_schema = cfg["tgt_schema"]
    process_date = cfg["process_date"]
    n_linhas = cfg["n_linhas"]
    seed = cfg["seed"]
    start_ts = cfg["start_ts"]
    taxa_duplicidade = cfg["taxa_duplicidade"]
    valid_status = cfg["valid_status"]

    tasks = [
        {
            "task_key": "init",
            "notebook_task": {
                "notebook_path": nb_map["00_init_schema_volume.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema},
            },
        },
        {
            "task_key": "dim_catalogo_link",
            "depends_on": [{"task_key": "init"}],
            "notebook_task": {
                "notebook_path": nb_map["10_dim_catalogo_produtos_link.ipynb"],
                "base_parameters": {"src_schema": src_schema, "tgt_schema": tgt_schema},
            },
        },
        {
            "task_key": "ingest_bronze",
            "depends_on": [{"task_key": "init"}],
            "notebook_task": {
                "notebook_path": nb_map["01_ingest_bronze_pedidos.ipynb"],
                "base_parameters": {
                    "tgt_schema": tgt_schema,
                    "process_date": process_date,
                    "n_linhas": n_linhas,
                    "seed": seed,
                    "start_ts": start_ts,
                    "taxa_duplicidade": taxa_duplicidade,
                },
            },
        },
        {
            "task_key": "silver_itens",
            "depends_on": [{"task_key": "dim_catalogo_link"}, {"task_key": "ingest_bronze"}],
            "notebook_task": {
                "notebook_path": nb_map["11_silver_vendas_itens_pedido.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema, "process_date": process_date},
            },
        },
        {
            "task_key": "silver_pedidos",
            "depends_on": [{"task_key": "silver_itens"}],
            "notebook_task": {
                "notebook_path": nb_map["12_silver_vendas_pedidos.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema},
            },
        },
        {
            "task_key": "gold_receita_produto_uf",
            "depends_on": [{"task_key": "silver_itens"}],
            "notebook_task": {
                "notebook_path": nb_map["20_gold_vendas_receita_mensal_produto_uf.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema, "valid_status": valid_status},
            },
        },
        {
            "task_key": "gold_kpis_mensais",
            "depends_on": [{"task_key": "silver_pedidos"}],
            "notebook_task": {
                "notebook_path": nb_map["21_gold_vendas_kpis_mensais.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema, "valid_status": valid_status},
            },
        },
        {
            "task_key": "report_smoke",
            "depends_on": [{"task_key": "gold_receita_produto_uf"}, {"task_key": "gold_kpis_mensais"}],
            "notebook_task": {
                "notebook_path": nb_map["90_report_smoke.ipynb"],
                "base_parameters": {"tgt_schema": tgt_schema},
            },
        },
    ]

    return {"name": cfg["job_name"], "tasks": tasks}


def main():
    cfg = json.loads(Path("workflow_config.json").read_text(encoding="utf-8"))

    # Autenticação automática pelo SDK (notebook/runtime ou config local)
    w = WorkspaceClient()

    folder = cfg["workspace_folder"].rstrip("/")
    mkdirs_if_needed(w, folder)

    if cfg.get("import_notebooks", True):
        local_dir = Path("../")
        overwrite = cfg.get("overwrite_notebooks", True)
        for nb in cfg["notebooks"]:
            ipynb_file = local_dir / nb["file"]
            ws_path = f"{folder}/{nb['path']}"
            print(f"Importing {ipynb_file} -> {ws_path}")
            import_notebook(w, ipynb_file, ws_path, overwrite=overwrite)

    settings = build_job_settings(cfg)
    job_id = find_job_id_by_name(w, cfg["job_name"])

    if job_id is None:
        print("Creating job...")
        out = w.api_client.do("POST", "/api/2.1/jobs/create", body=settings)
        print("Created job_id:", out.get("job_id"))
    else:
        print("Resetting job_id:", job_id)
        w.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": job_id, "new_settings": settings})
        print("Reset done.")


if __name__ == "__main__":
    main()