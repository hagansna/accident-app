import { Component, computed, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { VehiclesService } from './data-access/vehicles.service';
import { CardComponent } from './ui/card/card.component';

@Component({
  selector: 'app-vehicle-list',
  standalone: true,
  imports: [CommonModule, CardComponent],
  templateUrl: './vehicle-list.component.html',
  styleUrl: './vehicle-list.component.css',
})
export class VehicleListComponent {
  vehiclesService = inject(VehiclesService);

  nextPage() {
    this.vehiclesService.nextPage();
  }
  lastPage() {
    this.vehiclesService.lastPage();
  }
  previousPage() {
    this.vehiclesService.previousPage();
  }
  firstPage() {
    this.vehiclesService.firstPage();
  }
}
