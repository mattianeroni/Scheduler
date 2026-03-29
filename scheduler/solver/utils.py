from __future__ import annotations

import numpy as np
import cvxpy as cp
from scipy.sparse import csr_matrix


def sum_variables_from_indices(variables, indices):
    if indices.dtype != np.object_:
        return variables[indices]  # type: ignore
    num_rows, num_cols = indices.shape[0], variables.shape[0]
    row_idx = np.repeat(np.arange(num_rows), [len(row) for row in indices])
    col_idx = np.concatenate(indices)
    A = csr_matrix((np.ones(len(col_idx)), (row_idx, col_idx)), shape=(num_rows, num_cols))
    return cp.dot(A, variables)