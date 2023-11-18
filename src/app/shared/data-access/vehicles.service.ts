import { HttpClient } from '@angular/common/http';
import { Subject, concatMap, map, mergeMap, switchMap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Injectable, computed, effect, inject, signal } from '@angular/core';
import Vehicle from '../interfaces/vehicle.model';

interface State {
  vehicles: Vehicle[];
  loaded: boolean;
  error: string | null;
  page: number;
  totalPageCount: number;
}

const OFFSET = 10;

@Injectable({
  providedIn: 'root',
})
export class VehiclesService {
  private http = inject(HttpClient);

  private state = signal<State>({
    vehicles: [],
    loaded: false,
    error: null,
    page: 1,
    totalPageCount: 1,
  });

  vehicles = computed(() => this.state().vehicles);
  loaded = computed(() => this.state().loaded);
  error = computed(() => this.state().error);
  page = computed(() => this.state().page);
  totalPageCount = computed(() => this.state().totalPageCount);

  private page$ = new Subject<number>();
  private vehicles$ = this.page$.pipe(
    switchMap((page) => this.getVehicles(page))
  );
  private totalPageCount$ = this.getVehicleCount();
  constructor() {
    this.vehicles$.pipe(takeUntilDestroyed()).subscribe({
      next: (vehicles: Vehicle[]) =>
        this.state.update((state) => ({
          ...state,
          vehicles,
          loaded: true,
        })),
      error: (err: string) =>
        this.state.update((state) => ({ ...state, error: err })),
    });
    this.page$.pipe(takeUntilDestroyed()).subscribe({
      next: (page: number) =>
        this.state.update((state) => ({
          ...state,
          page,
          loaded: false,
        })),
    });
    this.totalPageCount$.pipe(takeUntilDestroyed()).subscribe({
      next: (totalPageCount: number) =>
        this.state.update((state) => ({
          ...state,
          totalPageCount,
        })),
    });

    this.page$.next(this.state().page);
  }

  getVehicleCount() {
    return this.http
      .get<number>('/api/count')
      .pipe(map((count) => Math.ceil(count / OFFSET)));
  }

  getVehicles(page = 1, size = OFFSET) {
    return this.http.get<Vehicle[]>(`/api/vehicles?page=${page}&size=${size}`);
  }

  nextPage() {
    if (this.state().page !== this.state().totalPageCount) {
      this.page$.next(this.state().page + 1);
    }
  }

  lastPage() {
    this.page$.next(this.state().totalPageCount);
  }

  previousPage() {
    if (this.state().page !== 1) {
      this.page$.next(this.state().page - 1);
    }
  }

  firstPage() {
    this.page$.next(1);
  }
}
