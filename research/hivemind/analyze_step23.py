"""Independently validate Step 23 raw traces, mechanism gates and paired outcomes.

Needs only the Python standard library. Invalid/incomplete evidence fails closed.
"""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_text())


def analyse_trial(path):
    require(not (path / "invalid.json").exists(), f"invalid trial: {path}")
    config, summary = read_json(path / "config.json"), read_json(path / "summary.json")
    lines = (path / "events.jsonl").read_text().splitlines()
    require(bool(lines), "empty trace")
    events = [json.loads(line) for line in lines]
    require(events[-1]["kind"] == "trace_closed", "trace is not closed")
    per_process = defaultdict(list)
    for event in events:
        require(math.isfinite(event["monotonic"]) and math.isfinite(event["wall_time"]), "invalid clock")
        per_process[event["pid"]].append(event["event_number"])
        require(not event["kind"].startswith("forbidden_"), "forbidden attacker action")
    for sequence in per_process.values():
        require(sequence == list(range(sequence[0], sequence[0]+len(sequence))), "missing or reordered process events")
    events.sort(key=lambda e: (e["monotonic"], e["pid"], e["event_number"]))
    ids = config["peer_ids"]
    names = {peer: name for name, peer in ids.items()}
    results, starts, searches, ended_searches, attempts = {}, {}, {}, set(), {}
    views = defaultdict(list)
    pair_attempts = set()
    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind == "step_start":
            name = detail["name"]
            require(name not in starts and ids[name] == event["peer"], "duplicate/mismatched step")
            require(detail["step_id"] == f"{name}:1", "invalid step id")
            starts[name] = event
        elif kind == "step_result":
            name = detail["name"]
            require(name in starts and name not in results and ids[name] == event["peer"], "invalid result identity")
            require(detail["step_id"] == f"{name}:1", "result step mismatch")
            require(event["monotonic"] >= starts[name]["monotonic"], "result before step")
            results[name] = {key: value for key, value in detail.items() if key != "search_id"}
        elif kind == "search_start":
            search = detail["search_id"]
            require(search not in searches and event["peer"] in names, "invalid search start")
            require(detail["step_id"] == f"{names[event['peer']]}:1", "search step mismatch")
            searches[search] = event["peer"]
        elif kind == "search_end":
            search = detail["search_id"]
            require(search in searches and search not in ended_searches, "invalid search end")
            require(searches[search] == event["peer"], "search owner mismatch")
            ended_searches.add(search)
        elif kind == "ad_view":
            views[(event["peer"], detail["search_id"])].append(event)
        elif kind == "leader_selected":
            search, expiration = detail["search_id"], detail["selected_expiration"]
            require(searches.get(search) == event["peer"] and search not in ended_searches, "selection outside search")
            pair = (event["peer"], search, event["other"], expiration)
            require(pair not in pair_attempts, "same leader/expiration selected twice in one search")
            pair_attempts.add(pair)
            seen = any(view["monotonic"] <= event["monotonic"] and
                       any(entry["peer"] == event["other"] and entry["expiration"] == expiration
                           for entry in view["detail"]["entries"])
                       for view in views[(event["peer"], search)])
            require(seen, "selected expiration absent from preceding native DHT views")
        elif kind == "join_start":
            attempt, search = detail["attempt_id"], detail["search_id"]
            require(attempt not in attempts and searches.get(search) == event["peer"], "invalid join attempt")
            expiration = detail["selected_expiration"]
            if expiration is not None:
                require((event["peer"], search, event["other"], expiration) in pair_attempts,
                        "attempt missing leader selection")
            attempts[attempt] = dict(start=event, end=None)
        elif kind == "join_end":
            attempt = attempts.get(detail["attempt_id"])
            require(attempt is not None and attempt["end"] is None, "invalid join end")
            require((event["peer"], event["other"], detail["search_id"]) ==
                    (attempt["start"]["peer"], attempt["start"]["other"], attempt["start"]["detail"]["search_id"]),
                    "join end identity mismatch")
            attempt["end"] = event
    require(set(searches) == ended_searches, "unfinished search")
    require(set(results) == set(ids) == set(starts), "missing step result")
    require(results == summary["outcomes"], "summary does not match raw results")
    require(all(row["end"] is not None for row in attempts.values()), "unfinished join stream")
    require(not any(row["status"] == "cancelled" for name, row in results.items() if name != "X"),
            "honest step cancelled")
    if results["X"]["status"] == "cancelled":
        require(summary["expected_x_cancel"], "unexpected X cancellation")
    for row in attempts.values():
        require(row["end"]["monotonic"] >= row["start"]["monotonic"], "negative waiting interval")

    assembled = {e["detail"]["group_id"]: e for e in events if e["kind"] == "assembled"}
    correct = {}
    incorrect = []
    for name, row in results.items():
        if row["status"] != "completed":
            correct[name] = False
            continue
        members = row["members"]
        require(bool(members) and ids[name] in members and set(members) <= set(names), "invalid gathered roster")
        for peer, data in members.items():
            expected_name = names[peer]
            require(data == dict(name=expected_name, step_id=f"{expected_name}:1",
                                 value=float(7 if expected_name == "X" else int(expected_name[1:]))),
                    "gathered input does not match fixed original input")
        require(any(set(event["detail"]["roster"]) == set(members) for event in assembled.values()),
                "completed roster was never assembled")
        expected = statistics.mean(data["value"] for data in members.values())
        require(len(row["actual"]) == config["tensor_size"], "wrong tensor size")
        own_correct = all(math.isfinite(value) and abs(value-expected) <= 1e-4+1e-5*abs(expected)
                          for value in row["actual"])
        if not own_correct:
            incorrect.append(name)
        correct[name] = own_correct and all(results[names[peer]].get("members") == members for peer in members)
    # A correctly returned group requires correct tensors from every contributor.
    verified = {name: okay and all(correct[names[peer]] for peer in results[name].get("members", {}))
                for name, okay in correct.items()}
    honest = [name for name in ids if name != "X"]
    horizon = config["correct_output_horizon"]
    times = {name: min(results[name]["elapsed"], horizon) if verified[name] else horizon for name in honest}
    honest_correct = sum(verified[name] and results[name]["elapsed"] <= horizon for name in honest)

    by_search = defaultdict(list)
    x_attempts = [row for row in attempts.values() if row["start"]["other"] == ids["X"]]
    for row in x_attempts:
        if row["start"]["detail"]["selected_expiration"] is not None:
            by_search[(row["start"]["peer"], row["start"]["detail"]["search_id"])].append(row["start"])
    retries = sum(max(0, len(rows)-1) for rows in by_search.values())
    renewal_retries = 0
    publications = [e for e in events if e["kind"] == "ad_finished" and e["peer"] == ids["X"]
                    and e["detail"]["active"]]
    mechanism_retry = False
    for rows in by_search.values():
        for previous, current in zip(rows, rows[1:]):
            expiration = current["detail"]["selected_expiration"]
            changed = expiration > previous["detail"]["selected_expiration"]
            published = any(e["detail"]["accepted"] and e["detail"]["expiration"] == expiration
                            and e["monotonic"] <= current["monotonic"] for e in publications)
            if changed and published:
                renewal_retries += 1
                if current["wall_time"] < rows[0]["detail"]["selected_expiration"]:
                    mechanism_retry = True
    if config["case"]["guard"] == "peer_id":
        require(all(len(rows) <= 1 for rows in by_search.values()), "PeerID guard allowed a repeated attempt")
    admissions = [e for e in events if e["kind"] == "join_reply" and e["peer"] == ids["X"]
                  and e["detail"]["code"] == "ACCEPTED"]
    released = [e for e in events if e["kind"] == "follower_released"]
    if config["case"]["response"] == "unreliable":
        require(len(admissions) == len(released), "unreleased unreliable followers")
        require(not any(e["peer"] == ids["X"] for e in assembled.values()), "unreliable X assembled")
    mechanism = config["case"]["phase"] == "mechanism"
    if mechanism:
        require(len(publications) == 2 and bool(x_attempts), "mechanism did not publish twice and receive a join")
        require(publications[0]["detail"]["accepted"], "initial mechanism publication failed")
        expiration0, expiration1 = (e["detail"]["expiration"] for e in publications)
        require(expiration1 == expiration0 if config["case"]["mode"] == "same" else expiration1 > expiration0,
                "mechanism expiration policy violated")
        require(any(e["kind"] == "observer_ready" for e in events), "mechanism start barrier missing")
        if config["case"]["mode"] == "same":
            require(retries == 0, "unchanged mechanism advertisement retried")
        else:
            require(publications[1]["detail"]["accepted"], "advanced mechanism publication failed")
    ads = [e for e in events if e["kind"] == "ad_started" and e["peer"] == ids["X"] and e["detail"]["active"]]
    return dict(case=config["case"]["name"], response=config["case"]["response"], mode=config["case"]["mode"],
                guard=config["case"]["guard"], phase=config["case"]["phase"], seed=config["seed"], pattern=config["pattern"],
                honest_correct=honest_correct, honest_count=len(honest), incorrect_steps=incorrect,
                penalized_seconds=statistics.mean(times.values()), honest_penalized_seconds=times,
                x_attempts=len(x_attempts), x_retries=retries, renewal_retries=renewal_retries,
                x_wait_seconds=sum(row["end"]["monotonic"]-row["start"]["monotonic"] for row in x_attempts),
                x_ad_attempts=len(ads), x_ad_accepted=sum(e["detail"]["accepted"] for e in publications),
                mechanism_retry_before_original_expiry=mechanism_retry, searches=len(searches), source=str(path))


def interval(values):
    rng = random.Random(230023)
    boot = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(10000))
    return dict(mean=statistics.mean(values), ci95=[boot[249], boot[9749]], seeds=len(values))


def contrasts(trials):
    indexed = {(row["seed"], row["pattern"], row["response"], row["mode"], row["guard"]): row for row in trials}
    output = {}
    for metric in ("penalized_seconds", "honest_correct", "x_retries", "x_wait_seconds"):
        primary, interaction = [], []
        for seed in sorted({row["seed"] for row in trials}):
            direct, adjusted = [], []
            for pattern in ("staggered", "bursty"):
                def value(response, mode):
                    return indexed[(seed, pattern, response, mode, "native")][metric]
                delta = value("unreliable", "refresh")-value("unreliable", "expire")
                direct.append(delta)
                adjusted.append(delta-(value("reliable", "refresh")-value("reliable", "expire")))
            primary.append(statistics.mean(direct))
            interaction.append(statistics.mean(adjusted))
        output[metric] = dict(primary=interval(primary), interaction=interval(interaction))
    return output


def analyse_suite(path):
    suite = read_json(path / "suite.json")
    provenance = read_json(path / "provenance.json")
    for name, digest in provenance["harness_sha256"].items():
        require(hashlib.sha256((path / "harness" / name).read_bytes()).hexdigest() == digest, "harness snapshot mismatch")
    expected = {(job["case"]["name"], job["seed"], job["pattern"]) for job in suite["jobs"]}
    trials = [analyse_trial(p.parent) for p in sorted(path.glob("*/summary.json"))]
    require(len(expected) == len(suite["jobs"]) == len(trials), "incomplete or duplicate suite")
    require({(r["case"], r["seed"], r["pattern"]) for r in trials} == expected, "suite configuration mismatch")
    for row in trials:
        config = read_json(Path(row["source"]) / "config.json")
        job = next(j for j in suite["jobs"] if (j["case"]["name"], j["seed"], j["pattern"]) ==
                   (row["case"], row["seed"], row["pattern"]))
        require(config["case"] == job["case"], "trial policy differs from suite")
    require(not any(row["incorrect_steps"] for row in trials), "incorrect tensors: interpretation paused")
    result = dict(phase=suite["phase"], trials=trials, trial_count=len(trials),
                  mechanism_pass=False, smoke_pass=False, harm_pass=False)
    if suite["phase"] == "mechanism":
        require(len(trials) == 10, "mechanism needs five paired seeds")
        advancing = [row for row in trials if row["mode"] == "advance"]
        result["mechanism_pairs_demonstrated"] = sum(row["mechanism_retry_before_original_expiry"] for row in advancing)
        result["mechanism_pass"] = result["mechanism_pairs_demonstrated"] >= 4
    elif suite["phase"] in ("smoke", "confirm"):
        expected_count = 16 if suite["phase"] == "smoke" else 160
        require(len(trials) == expected_count, "wrong comparison trial count")
        result["contrasts"] = contrasts(trials)
        result["smoke_pass"] = suite["phase"] == "smoke"
        if suite["phase"] == "confirm":
            time_effect = result["contrasts"]["penalized_seconds"]
            count_effect = result["contrasts"]["honest_correct"]
            time_harm = time_effect["primary"]["mean"] >= 0.5 and all(time_effect[k]["ci95"][0] > 0 for k in ("primary", "interaction"))
            count_harm = count_effect["primary"]["mean"] <= -0.5 and all(count_effect[k]["ci95"][1] < 0 for k in ("primary", "interaction"))
            result["harm_pass"] = any(row["renewal_retries"] for row in trials if row["response"] == "unreliable") and (time_harm or count_harm)
    else:
        require(suite["phase"] == "diagnostic" and len(trials) == 48, "wrong diagnostic suite")
        result["diagnostic_aggregate"] = aggregate(trials)
    result["aggregate"] = aggregate(trials)
    return result


def aggregate(trials):
    grouped = defaultdict(list)
    for row in trials:
        grouped[row["case"]].append(row)
    return [dict(case=case, trials=len(rows), honest_correct=sum(r["honest_correct"] for r in rows),
                 honest_count=sum(r["honest_count"] for r in rows), x_retries=sum(r["x_retries"] for r in rows),
                 renewal_retries=sum(r["renewal_retries"] for r in rows), x_ad_attempts=sum(r["x_ad_attempts"] for r in rows),
                 x_wait_seconds=sum(r["x_wait_seconds"] for r in rows),
                 mean_penalized_seconds=statistics.mean(r["penalized_seconds"] for r in rows))
            for case, rows in sorted(grouped.items())]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyse_suite(args.suite)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "trials"}, indent=2))


if __name__ == "__main__":
    main()
