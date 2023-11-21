import { HttpClient } from '@angular/common/http';
import { Injectable, computed, inject, signal } from '@angular/core';
import { Frequency } from '../interfaces/frequency';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Subject, map, merge, pairwise, switchMap } from 'rxjs';

interface State {
	frequencies: Frequency[];
	loaded: boolean;
	page: number;
	error: string | null;
	searchTerm: string;
}

const OFFSET = 10;

@Injectable({
	providedIn: 'root',
})
export class FrequencyListService {
	private http = inject(HttpClient);

	private state = signal<State>({
		frequencies: [],
		page: 1,
		loaded: false,
		error: null,
		searchTerm: '',
	});

	frequencies = computed(() => this.state().frequencies);
	loaded = computed(() => this.state().loaded);
	error = computed(() => this.state().error);
	page = computed(() => this.state().page);
	searchTerm = computed(() => this.state().searchTerm);

	private page$ = new Subject<number>();
	private searchTerm$ = new Subject<string>();
	private frequencies$ = merge(
		this.page$.pipe(
			switchMap((page) =>
				this.getFrequencies(page, OFFSET, this.state().searchTerm)
			)
		),
		this.searchTerm$.pipe(
			pairwise(),
			switchMap(([oldSearchTerm, newSearchTerm]) => {
				if (newSearchTerm != oldSearchTerm) {
					this.state.update((state) => ({
						...state,
						page: 1,
						loaded: false,
					}));
				}
				return this.getFrequencies(
					this.state().page,
					OFFSET,
					newSearchTerm
				);
			})
		)
	);
	constructor() {
		this.frequencies$.pipe(takeUntilDestroyed()).subscribe({
			next: (frequencies: Frequency[]) =>
				this.state.update((state) => ({
					...state,
					frequencies,
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
		this.searchTerm$.pipe(takeUntilDestroyed()).subscribe({
			next: (searchTerm: string) =>
				this.state.update((state) => ({
					...state,
					searchTerm,
					loaded: false,
				})),
		});
		this.page$.next(this.state().page);
	}

	getFrequencies(
		page: number,
		offset: number = OFFSET,
		searchTerm: string = ''
	) {
		return this.http.get<Frequency[]>(
			`/api/frequency?page=${page}&offset=${offset}&searchTerm=${searchTerm}`
		);
	}

	nextPage() {
		this.page$.next(this.state().page + 1);
	}
	previousPage() {
		if (this.state().page !== 1) {
			this.page$.next(this.state().page - 1);
		}
	}
	filterTable(searchTerm: string) {
		this.searchTerm$.next(searchTerm);
	}
}
