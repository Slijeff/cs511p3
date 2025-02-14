from ray.util.state import summarize_tasks


def success(counter):
    finished = 0
    summary = summarize_tasks(timeout=60, _explain=True)['cluster']['summary']
    print(summary)
    for value in summary.values():
        if 'FINISHED' in value['state_counts']:
            finished += 1
    return counter == finished
