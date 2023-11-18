import { HttpClient } from '@angular/common/http';
import { Subject, concatMap, map, mergeMap, switchMap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Injectable, computed, effect, inject, signal } from '@angular/core';
import Accident from '../interfaces/accidents.model';

interface State {
  accidents: Accident[],
  loaded: boolean,
  error: string | null,
  page: number,
  totalPageCount: number
}

const OFFSET = 10

@Injectable({
  providedIn: 'root'
})
export class AccidentsService {
  private http = inject(HttpClient)

  private state = signal<State>({
    accidents: [],
    loaded: false,
    error: null,
    page: 1,
    totalPageCount: 1
  })

  accidents = computed(() => this.state().accidents);
  loaded = computed(() => this.state().loaded);
  error = computed(() => this.state().error);
  page = computed(() => this.state().page);
  totalPageCount = computed(() => this.state().totalPageCount);

  
  private page$ = new Subject<number>()
  private accidents$ = this.page$.pipe(switchMap((page) => this.getAccidents(page)))
  private totalPageCount$ = this.getAccidentCount()
  constructor() {
    this.accidents$.pipe(takeUntilDestroyed()).subscribe({
      next: (accidents: Accident[]) => 
        this.state.update((state) => ({
          ...state,
          accidents,
          loaded: true,
        })),
      error: (err: string) => this.state.update((state) => ({...state, error: err})),
    })
    this.page$.pipe(takeUntilDestroyed()).subscribe({
      next: (page:number) =>
        this.state.update((state) => ({
          ...state,
          page,
          loaded: false,
        }))
    })
    this.totalPageCount$.pipe(takeUntilDestroyed()).subscribe({
      next: (totalPageCount: number) =>
        this.state.update((state) => ({
          ...state,
          totalPageCount
        }))
    })

    this.page$.next(this.state().page)
  }

  getAccidentCount() {
    return this.http.get<number>('/api/count').pipe(map((count) => Math.ceil(count / OFFSET)))
  }

  getAccidents(page = 1, size = OFFSET) {
    return this.http.get<Accident[]>(`/api/accidents?page=${page}&size=${size}`)
  }

  nextPage() {
    if (this.state().page !== this.state().totalPageCount) {
      this.page$.next(this.state().page + 1)
    }
  }

  lastPage() {
    this.page$.next(this.state().totalPageCount)
  }

  previousPage() {
    if (this.state().page !== 1) {
      this.page$.next(this.state().page - 1)
    }
  }

  firstPage() {
    this.page$.next(1)
  }
}
