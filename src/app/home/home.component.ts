import { Component, computed, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AccidentsService } from '../shared/data-access/accidents.service';
import { CardComponent } from './ui/card/card.component';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [CommonModule, CardComponent],
  templateUrl: './home.component.html',
  styleUrl: './home.component.css'
})
export class HomeComponent {
  accidentsService = inject(AccidentsService)

  nextPage() {
    this.accidentsService.nextPage();
  }
  lastPage() {
    this.accidentsService.lastPage();
  }
  previousPage() {
    this.accidentsService.previousPage();
  }
  firstPage() {
    this.accidentsService.firstPage();
  }
}
