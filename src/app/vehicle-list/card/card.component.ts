import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import Vehicle from '../../shared/interfaces/vehicle.model';

@Component({
  selector: 'app-card',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './card.component.html',
  styleUrl: './card.component.css',
})
export class CardComponent {
  @Input() vehicle!: Vehicle;
}
