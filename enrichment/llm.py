import json
import re

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL = "Qwen/Qwen2.5-1.5B-Instruct"

_tok = None
_model = None


def _load():
    """Charge le modèle au premier appel seulement."""
    global _tok, _model
    if _model is None:
        torch.set_num_threads(6)
        _tok = AutoTokenizer.from_pretrained(MODEL)
        _model = AutoModelForCausalLM.from_pretrained(MODEL, dtype=torch.float32)
    return _tok, _model


SYSTEM = (
    "Tu es un analyste financier expert. Tu classes des actualités (en français ou "
    "en anglais) et tu réponds UNIQUEMENT par un objet JSON valide, sans texte autour."
)

USER_TEMPLATE = (
    "Catégories type_evenement :\n"
    "- politique : élections, gouvernements, géopolitique, conflits\n"
    "- economique : résultats, emploi, inflation, prix des matières premières, marchés\n"
    "- technologique : innovations, IA, semi-conducteurs, produits tech\n"
    "- reglementaire : lois, régulateurs, enquêtes, sanctions, normes\n\n"
    "impact_attendu = effet probable sur le cours des actifs concernés :\n"
    "- hausse : nouvelle positive (bons résultats, expansion, accord favorable)\n"
    "- baisse : nouvelle négative (pertes, licenciements, enquête, sanction, chute)\n"
    "- neutre : incertain ou sans effet clair\n\n"
    "Réponds par un JSON aux clés : type_evenement, impact_attendu, "
    "actifs_concernes (liste), localisation, resume (une phrase).\n\n"
    "Exemples :\n"
    'Titre: "Volkswagen prévoit de supprimer 15% de ses effectifs"\n'
    '{"type_evenement":"economique","impact_attendu":"baisse","actifs_concernes":["Volkswagen"],"localisation":"Allemagne","resume":"Plan social massif chez Volkswagen."}\n'
    'Titre: "Apple dévoile des résultats trimestriels record"\n'
    '{"type_evenement":"economique","impact_attendu":"hausse","actifs_concernes":["Apple"],"localisation":"États-Unis","resume":"Bénéfices record pour Apple."}\n'
    'Titre: "La BCE laisse ses taux inchangés, conformément aux attentes"\n'
    '{"type_evenement":"economique","impact_attendu":"neutre","actifs_concernes":["secteur bancaire"],"localisation":"Europe","resume":"Statu quo monétaire de la BCE."}\n\n'
    "Classe maintenant :\n"
)


def _extract_json(text: str) -> dict | None:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


_IMPACT_NORM = {"neutral": "neutre", "up": "hausse", "down": "baisse"}


def _normalize(meta: dict | None) -> dict | None:
    """Uniformise les valeurs (le LLM glisse parfois vers l'anglais : 'neutral')."""
    if not meta:
        return meta
    imp = str(meta.get("impact_attendu", "")).lower()
    meta["impact_attendu"] = _IMPACT_NORM.get(imp, imp)
    return meta


def enrich(title: str, summary: str) -> dict | None:
    """Génère les métadonnées d'un article via le LLM."""
    tok, model = _load()
    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": USER_TEMPLATE + f'Titre: "{title}"\nRésumé: {summary}'},
    ]
    prompt = tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tok(prompt, return_tensors="pt")
    out = model.generate(**inputs, max_new_tokens=220, do_sample=False)
    generated = tok.decode(out[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
    return _normalize(_extract_json(generated))
