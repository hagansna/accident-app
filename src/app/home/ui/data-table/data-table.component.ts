import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Frequency } from '../../interfaces/frequency';
import { MatTableModule } from '@angular/material/table';
import { Observable } from 'rxjs';

@Component({
  selector: 'app-data-table',
  standalone: true,
  imports: [CommonModule, MatTableModule],
  templateUrl: './data-table.component.html',
  styleUrl: './data-table.component.css',
})
export class DataTableComponent {
  @Input() frequencies$!: Observable<Frequency[]>;
  @Input() displayedColumns!: string[];
}
