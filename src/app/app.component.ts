import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet } from '@angular/router';
import { VehicleListComponent } from './vehicle-list/vehicle-list.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, RouterOutlet, VehicleListComponent],
  template: `<app-vehicle-list />`,
  styleUrls: ['./app.component.css'],
})
export class AppComponent {
  title = 'accident-app';
}
