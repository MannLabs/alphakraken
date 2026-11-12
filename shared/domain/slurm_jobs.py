"""Slurm job constants."""


class SlurmJobQueueStates:
    """States of Slurm jobs set by AlphaKraken, in addition to the ones reported by `squeue`."""

    # the job was in the queue, but is no more (e.g. finished)
    LEFT_QUEUE = "LEFT_QUEUE"


# expired entries are deleted by MongoDB
SLURM_JOB_EXPIRY_S = 7 * 24 * 60 * 60
