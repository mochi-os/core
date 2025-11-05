// Static mock data for tasks
// In production, replace with actual API data

const statuses = [
  'todo',
  'in progress',
  'done',
  'canceled',
  'backlog',
] as const
const labels = ['bug', 'feature', 'documentation'] as const
const priorities = ['low', 'medium', 'high'] as const

const sampleTitles = [
  'Fix authentication bug in login flow',
  'Implement dark mode toggle',
  'Add user profile page',
  'Update documentation for API endpoints',
  'Optimize database queries',
  'Create responsive mobile layout',
  'Fix memory leak in data table',
  'Add unit tests for auth service',
  'Improve error handling',
  'Refactor component structure',
]

const sampleDescriptions = [
  'This task requires attention to detail and thorough testing.',
  'Need to ensure backward compatibility while implementing this feature.',
  'Coordinate with backend team before starting implementation.',
  'Review design mockups before proceeding.',
  'Ensure all edge cases are handled properly.',
]

const sampleAssignees = [
  'John Smith',
  'Jane Doe',
  'Michael Johnson',
  'Sarah Williams',
  'David Brown',
  'Emily Davis',
  'Robert Miller',
  'Jessica Wilson',
]

function randomElement<T>(array: readonly T[]): T {
  return array[Math.floor(Math.random() * array.length)]
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function randomDate(start: Date, end: Date): Date {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

export const tasks = Array.from({ length: 100 }, () => {
  return {
    id: `TASK-${randomInt(1000, 9999)}`,
    title: randomElement(sampleTitles),
    status: randomElement(statuses),
    label: randomElement(labels),
    priority: randomElement(priorities),
    createdAt: randomDate(new Date(2020, 0, 1), new Date()),
    updatedAt: randomDate(new Date(Date.now() - 7 * 24 * 60 * 60 * 1000), new Date()),
    assignee: randomElement(sampleAssignees),
    description: randomElement(sampleDescriptions),
    dueDate: randomDate(new Date(), new Date(Date.now() + 90 * 24 * 60 * 60 * 1000)),
  }
})
