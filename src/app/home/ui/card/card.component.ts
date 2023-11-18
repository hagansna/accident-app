import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import Accident from '../../../shared/interfaces/accidents.model';

@Component({
  selector: 'app-card',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './card.component.html',
  styleUrl: './card.component.css'
})
export class CardComponent {
  @Input() accident!: Accident;
}
