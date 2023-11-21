import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet } from '@angular/router';
import { VehicleListComponent } from './vehicle-list/vehicle-list.component';
import { HomeComponent } from './home/home.component';

@Component({
  selector: 'app-root',
  standalone: true,
  template: `<app-home />`,
  styleUrls: ['./app.component.css'],
  imports: [CommonModule, RouterOutlet, VehicleListComponent, HomeComponent],
})
export class AppComponent {
  title = 'accident-app';
}
