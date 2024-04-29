#pragma once
#include <cstring>

#include "common.h"

// NOLINTBEGIN

typedef struct {
  int freq;
  int time;
} peak_t;

typedef struct {
  int start;
  int width;
  int lines_read;
  int peaks_c;
  peak_t peaksbuff[PFRAME_CELLS * 100];
} peaks_batch_t;

typedef struct {
  int cell_x;
  int cell_y;
  float val;
  bool ok;
} candidate_t;

typedef struct {
  float data[PFRAME_WIDTH][PFRAME_HEIGHT];
  candidate_t candidates[PFRAME_CELLS];
} pframe_t;

typedef struct {
  pframe_t prev;
  pframe_t curr;
  pframe_t next;
  int next_sframes;
  int filled_pframes;
  int column_id;
} peaks_gen_t;

inline void peaks_init(peaks_gen_t *gen) {
  gen->next_sframes = 0;
  gen->filled_pframes = 0;
}

inline int abs_int(int a) { return a > 0 ? a : -a; }

inline void compare(candidate_t *c1, candidate_t *c2, int dx, int dy) {
  if (c1->val == PFRAME_MIN_VAL || c2->val == PFRAME_MIN_VAL) return;

  bool close_x = abs_int(dx * PFRAME_WIDTH + c2->cell_x - c1->cell_x) < MASK_RX;
  bool close_y =
      abs_int(dy * PFRAME_CELL_HEIGHT + c2->cell_y - c1->cell_y) < MASK_RY;
  if (close_x && close_y) {
    if (c1->val >= c2->val) {
      c2->ok = false;
    } else {
      c1->ok = false;
    }
  }
}

inline bool is_maximum(float val, pframe_t *frame, int from_y, int to_y,
                       int from_x, int to_x) {
  if (from_y < 0) from_y = 0;
  if (from_x < 0) from_x = 0;
  if (to_y > PFRAME_HEIGHT) to_y = PFRAME_HEIGHT;
  if (to_x > PFRAME_WIDTH) to_x = PFRAME_WIDTH;

  for (int x = from_x; x < to_x; x++)
    for (int y = from_y; y < to_y; y++)
      if (frame->data[x][y] > val) return false;
  return true;
}

inline void _peaks_next_pframe(peaks_gen_t *gen) {
  pframe_t tmp = gen->prev;
  gen->prev = gen->curr;
  gen->curr = gen->next;
  gen->next = tmp;
  // gen->curr
  // memcpy(&gen->prev, &gen->curr, sizeof(pframe_t));
  // memcpy(&gen->curr, &gen->next, sizeof(pframe_t));
  gen->next_sframes = 0;
}

/**
 * returns number of peaks written
 */
inline bool peaks_consume_sframe(peaks_gen_t *gen, float *sframe, peak_t *peaks,
                                 size_t *peakc) {
  *peakc = 0;

  // for (int i = 0; i < PFRAME_HEIGHT; i++) {
  //   gen->next.data[gen->next_sframes][i] = sframe[i];
  // }
  // float *f = ;
  memcpy(gen->next.data[gen->next_sframes], sframe,
         sizeof(float) * PFRAME_HEIGHT);
  gen->next_sframes++;

  // still filling next pframe
  if (gen->next_sframes < PFRAME_WIDTH) return false;

  // just starting, first results are after 3 pframes collected
  gen->filled_pframes++;
  if (gen->filled_pframes < 3) {
    _peaks_next_pframe(gen);
    return false;
  }
  gen->filled_pframes = 3;

  // precompute gen->next.candidates
  for (int yi = 0; yi < PFRAME_CELLS; yi++) {
    size_t yv = yi * PFRAME_CELL_HEIGHT;

    candidate_t *cand = &gen->next.candidates[yi];

    /**
     * FIND MAXIMUM
     */
    cand->val = PFRAME_MIN_VAL;
    cand->ok = false;

    for (int x = 0; x < PFRAME_WIDTH; x++) {
      for (int y = 0; y < PFRAME_CELL_HEIGHT; y++) {
        float val = gen->next.data[x][yv + y];
        // logi("%f %f\t", cand->val, val);
        if (val > cand->val) {
          cand->val = val;
          cand->cell_x = x;
          cand->cell_y = y;
          cand->ok = true;
          // logi("candidate OK  ###################33---\n");
        }
      }
    }

    /**
     * CHECK NEIGHBORING CANDIDATES
     *
     * checking candidates on left and above if they exist
     *    x x .
     *    x o .
     *    x . .
     * if checked candidates are within their range one of them will be kicked
     * (not ok) in case of draws, left second candidate is kicked
     */
    if (yi > 0) compare(&gen->curr.candidates[yi - 1], cand, 1, 1);
    compare(&gen->curr.candidates[yi], cand, 1, 0);
    if (yi + 1 < PFRAME_CELLS)
      compare(&gen->curr.candidates[yi + 1], cand, 1, -1);
    if (yi > 0) compare(&gen->next.candidates[yi - 1], cand, 0, 1);
  }

  for (int yi = 0; yi < PFRAME_CELLS; yi++) {
    int yv = yi * PFRAME_CELL_HEIGHT;

    candidate_t *cand = &gen->curr.candidates[yi];
    // candidate already kicked
    if (!cand->ok) continue;

    // check if really maximum
    size_t from_y = yv + cand->cell_y - PFRAME_CELL_HEIGHT + 1;
    size_t to_y = yv + cand->cell_y + PFRAME_CELL_HEIGHT;
    size_t from_x = cand->cell_x - PFRAME_WIDTH + 1;
    size_t to_x = cand->cell_x + PFRAME_WIDTH;

    if (!is_maximum(cand->val, &gen->prev, from_y, to_y, from_x + PFRAME_WIDTH,
                    to_x + PFRAME_WIDTH))
      continue;
    if (!is_maximum(cand->val, &gen->curr, from_y, to_y, from_x, to_x))
      continue;
    if (!is_maximum(cand->val, &gen->next, from_y, to_y, from_x - PFRAME_WIDTH,
                    to_x - PFRAME_WIDTH))
      continue;

    // FOUND A PEAK!
    peaks[*peakc] = {(yv + cand->cell_y), cand->cell_x};
    *peakc = *peakc + 1;
  }

  // make space to collect next pframe
  _peaks_next_pframe(gen);
  return true;
}

// NOLINTEND