import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FrequencyListService } from './data-access/frequency-list.service';
import { DataTableComponent } from './ui/data-table/data-table.component';
import { toObservable } from '@angular/core/rxjs-interop';
import { SearchBarComponent } from './ui/search-bar/search-bar.component';

@Component({
	selector: 'app-home',
	standalone: true,
	templateUrl: './home.component.html',
	styleUrl: './home.component.css',
	imports: [CommonModule, DataTableComponent, SearchBarComponent],
})
export class HomeComponent {
	frequencyListService = inject(FrequencyListService);
	frequencies$ = toObservable(this.frequencyListService.frequencies);
	displayedColumns: string[] = ['Make', 'Model', 'Count'];

	nextPage() {
		this.frequencyListService.nextPage();
	}

	previousPage() {
		this.frequencyListService.previousPage();
	}

	filterTable(searchTerm: string) {
		this.frequencyListService.filterTable(searchTerm);
	}
}
